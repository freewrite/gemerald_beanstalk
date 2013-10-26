require 'securerandom'
require 'socket'

class GemeraldBeanstalk::Beanstalk
  BEANSTALK_COMMANDS = %w[
    bury delete ignore kick kick-job list-tubes list-tube-used list-tubes-watched
    pause-tube peek peek-buried peek-delayed peek-ready put quit release reserve
    reserve-with-timeout stats stats-job stats-tube touch use watch
  ]

  CONNECTION_PARSER_ACCESSIBLE_COMMANDS = %w[bad_format!]

  ALL_COMMANDS = BEANSTALK_COMMANDS + CONNECTION_PARSER_ACCESSIBLE_COMMANDS

  underscored_method_names = {
    'kick-job' => 'kick_job', 'list-tubes' => 'list_tubes', 'list-tube-used' => 'list_tube_used',
    'list-tubes-watched' => 'list_tubes_watched', 'pause-tube' => 'pause_tube', 'peek-buried' => 'peek_buried',
    'peek-delayed' => 'peek_delayed', 'peek-ready' => 'peek_ready', 'reserve-with-timeout' => 'reserve_with_timeout',
    'stats-job' => 'stats_job', 'stats-tube' => 'stats_tube',
  }

  COMMAND_METHOD_NAMES = Hash[ALL_COMMANDS.zip(ALL_COMMANDS)].merge!(underscored_method_names)

  CONNECTION_SPECIFIC_COMMANDS = %w[
    bury delete ignore kick list-tube-used list-tubes-watched peek-buried peek-delayed
    peek-ready put quit release reserve reserve-with-timeout touch use watch
  ]

  STATS_COMMANDS = %w[
    bury delete ignore kick list-tube-used list-tubes list-tubes-watched pause-tube peek peek-buried
    peek-delayed peek-ready put release reserve stats stats-job stats-tube use watch
  ]

  BAD_FORMAT = "BAD_FORMAT\r\n"
  BURIED = "BURIED\r\n"
  CRLF = "\r\n"
  DEADLINE_SOON = "DEADLINE_SOON\r\n"
  DELETED = "DELETED\r\n"
  EXPECTED_CRLF = "EXPECTED_CRLF\r\n"
  JOB_TOO_BIG = "JOB_TOO_BIG\r\n"
  KICKED = "KICKED\r\n"
  NOT_FOUND = "NOT_FOUND\r\n"
  NOT_IGNORED = "NOT_IGNORED\r\n"
  PAUSED = "PAUSED\r\n"
  RELEASED = "RELEASED\r\n"
  TIMED_OUT = "TIMED_OUT\r\n"
  TOUCHED = "TOUCHED\r\n"
  UNKNOWN_COMMAND = "UNKNOWN_COMMAND\r\n"

  JOB_INACTIVE_STATES = GemeraldBeanstalk::Job::INACTIVE_STATES
  JOB_RESERVED_STATES = GemeraldBeanstalk::Job::RESERVED_STATES

  attr_reader :address

  def connect(connection = nil)
    beanstalk_connection = GemeraldBeanstalk::Connection.new(self, connection)
    @connections << beanstalk_connection
    adjust_stats_key('total-connections')
    return beanstalk_connection
  end


  def disconnect(connection)
    tube(connection.tube_used).stop_use
    @reserved[connection].each do |job|
      job.release(connection, job.priority, 0, false)
    end
    @reserved.delete(connection)
    @connections.delete(connection)
    connection.close_connection
  end


  def execute(connection, command = nil, *command_params)
    return UNKNOWN_COMMAND unless valid_command?(command)

    adjust_stats_key("cmd-#{command}") if STATS_COMMANDS.include?(command)

    command_params.unshift(connection) if connection_specific_command?(command)

    return BAD_FORMAT if COMMAND_METHOD_PARAMETER_COUNTS[command] != command_params.length

    return send(COMMAND_METHOD_NAMES[command], *command_params)
  end


  def initialize(address, max_job_size = 65535)
    @max_job_size = max_job_size
    @address = address
    @connections = ThreadSafe::Array.new
    @delayed = ThreadSafe::Array.new
    @id = SecureRandom.base64(16)
    @jobs = GemeraldBeanstalk::Jobs.new
    @mutex = Mutex.new
    @paused = ThreadSafe::Array.new
    @reserved = ThreadSafe::Cache.new {|reserved, key| reserved[key] = [] }
    @stats = ThreadSafe::Hash.new(0)
    @tubes = ThreadSafe::Cache.new
    @up_at = Time.now.to_f

    tube('default', :create_if_missing)
  end


  def register_job_timeout(connection, job)
    @reserved[connection].delete(job)
    adjust_stats_key('job-timeouts')
    honor_reservations(job)
  end


  def update_state
    waiting_connections.each do |connection|
      if connection.waiting? && deadline_pending?(connection)
        message_for_connection = DEADLINE_SOON
      elsif connection.timed_out?
        message_for_connection = TIMED_OUT
      end

      next if message_for_connection.nil?
      cancel_reservations(connection)
      connection.transmit(message_for_connection)
    end
    @reserved.values.flatten.each(&:state)
    @delayed.keep_if do |job|
      if job.delayed?
        true
      else
        honor_reservations(job)
        false
      end
    end
    @paused.keep_if do |tube|
      if tube.paused?
        true
      else
        honor_reservations(tube)
        false
      end
    end
  end


  protected

  def active_tubes
    tubes = {}
    @tubes.each_pair { |tube_name, tube| tubes[tube_name] = tube if tube.active? }
    return tubes
  end


  def adjust_stats_key(key, adjustment = 1)
    @stats[key] += adjustment
  end


  def bad_format!
    return BAD_FORMAT
  end


  def bury(connection, job_id, priority)
    job = find_job(job_id, :only => JOB_RESERVED_STATES)
    return NOT_FOUND if job.nil? || !job.bury(connection, priority)

    @reserved[connection].delete(job)
    return BURIED
  end


  def cancel_reservations(connection)
    connection.tubes_watched.each do |tube_name|
      tube(tube_name).cancel_reservation(connection)
    end
    return connection
  end


  def connection_specific_command?(command)
    return CONNECTION_SPECIFIC_COMMANDS.include?(command)
  end


  def deadline_pending?(connection)
    return @reserved[connection].any?(&:deadline_pending?)
  end


  def delete(connection, job_id)
    job = find_job(job_id)
    return NOT_FOUND if job.nil? || !job.delete(connection)

    tube(job.tube_name).delete(job)
    @jobs[job.id - 1] = nil
    @reserved[connection].delete(job) if JOB_RESERVED_STATES.include?(job.state)

    return DELETED
  end


  def find_job(job_id, options = {})
    only = Array(options[:only])
    except = Array(options[:except]).unshift(:deleted)

    job = @jobs[job_id.to_i - 1]

    return nil if job.nil? || except.include?(job.state)
    return (only.empty? || only.include?(job.state)) ? job : nil
  end


  def honor_reservations(job_or_tube, tube = nil)
    if job_or_tube.is_a?(GemeraldBeanstalk::Job)
      job = job_or_tube
      tube ||= tube(job.tube_name)
    elsif job_or_tube.is_a?(GemeraldBeanstalk::Tube)
      tube = job_or_tube
      job = tube.next_job
    end

    while job && (next_reservation = tube.next_reservation)
      next unless try_dispatch(next_reservation, job)
      job = tube.next_job
    end
  end


  def ignore(connection, tube_name)
    return NOT_IGNORED if (watched_count = connection.ignore(tube_name)).nil?
    tube(tube_name).ignore
    return "WATCHING #{watched_count}\r\n"
  end


  def kick(connection, limit)
    limit = limit.to_i
    kicked = 0
    JOB_INACTIVE_STATES.each do |job_state|
      # GTE to handle negative limits
      break if kicked >= limit
      until (job = tube(connection.tube_used).next_job(job_state, :peek)).nil?
        kicked += 1 if job.kick
        break if kicked == limit
      end
    end

    return "KICKED #{kicked}\r\n"
  end


  def kick_job(job_id)
    job = find_job(job_id, :only => JOB_INACTIVE_STATES)
    return (!job.nil? && job.kick) ? KICKED : NOT_FOUND
  end


  def list_tubes
    return tube_list(active_tubes.keys)
  end


  def list_tube_used(connection)
    return "USING #{connection.tube_used}\r\n"
  end


  def list_tubes_watched(connection)
    return tube_list(connection.tubes_watched)
  end


  def next_job(connection, state = :ready)
    best_candidate = nil
    connection.tubes_watched.each do |tube_name|
      candidate = tube(tube_name).next_job(state)
      next if candidate.nil?

      best_candidate = candidate if best_candidate.nil? || candidate < best_candidate
    end

    return best_candidate
  end


  def pause_tube(tube_name, delay)
    return NOT_FOUND if (tube = tube(tube_name)).nil?

    tube.pause(delay)
    @paused << tube
    return PAUSED
  end


  def peek(job_id)
    job = find_job(job_id)
    return job.nil? ? NOT_FOUND : "FOUND #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
  end


  def peek_by_state(connection, state)
    job = tube(connection.tube_used).next_job(state, :peek)
    return job.nil? ? NOT_FOUND : "FOUND #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
  end


  def peek_buried(connection)
    return peek_by_state(connection, :buried)
  end


  def peek_delayed(connection)
    return peek_by_state(connection, :delayed)
  end


  def peek_ready(connection)
    return peek_by_state(connection, :ready)
  end


  def put(connection, priority, delay, ttr, bytes, body)
    bytes = bytes.to_i
    return JOB_TOO_BIG if bytes > @max_job_size
    return EXPECTED_CRLF if body.length - 2 != bytes || body.slice!(-2, 2) != CRLF

    id = job = tube = nil
    @mutex.synchronize do
      id = @jobs.total_jobs + 1
      job = GemeraldBeanstalk::Job.new(self, id, connection.tube_used, priority, delay, ttr, bytes, body)
      @jobs.enqueue(job)
      tube = tube(connection.tube_used)
      tube.put(job)
    end
    connection.producer = true

    # Send async so client doesn't wait while we check if job can be immediately dispatched
    connection.transmit("INSERTED #{id}\r\n")

    if job.ready?
      honor_reservations(job, tube)
    elsif job.delayed?
      @delayed << job
    end
    return nil
  end


  def quit(connection)
    disconnect(connection)
    return nil
  end


  def release(connection, job_id, priority, delay)
    job = find_job(job_id)
    return NOT_FOUND if job.nil?
    return BAD_FORMAT unless job.release(connection, priority.to_i, delay.to_i)

    @reserved[connection].delete(job)
    @delayed << job if job.delayed?
    return RELEASED
  end


  def reserve(connection)
    reserve_job(connection)
    return nil
  end


  def reserve_job(connection, timeout = nil)
    connection.worker = true

    if deadline_pending?(connection)
      connection.transmit(DEADLINE_SOON)
      return true
    end

    connection.tubes_watched.each do |tube_name|
      tube(tube_name).reserve(connection)
    end
    connection.wait([nil, 0].include?(timeout) ? nil : Time.now.to_f + timeout)

    dispatched = false
    while !dispatched
      break if (job = next_job(connection)).nil?
      dispatched = try_dispatch(connection, job)
    end

    return dispatched
  end


  def reserve_with_timeout(connection, timeout)
    return nil if reserve_job(connection, timeout.to_i)
    connection.wait_timed_out
    return TIMED_OUT
  end


  def stats
    job_stats = @jobs.counts_by_state
    connection_stats = {
      'current-producers' => 0,
      'current-waiting' => 0,
      'current-workers' => 0,
    }
    @connections.each do |connection|
      connection_stats['current-producers'] += 1 if connection.producer?
      connection_stats['current-waiting'] += 1 if connection.waiting?
      connection_stats['current-workers'] += 1 if connection.worker?
    end
    connection_stats['current-connections'] = @connections.length
    stats = {
      'current-jobs-urgent' => job_stats['current-jobs-urgent'],
      'current-jobs-ready' => job_stats['current-jobs-ready'],
      'current-jobs-reserved' => job_stats['current-jobs-reserved'],
      'current-jobs-delayed' => job_stats['current-jobs-delayed'],
      'current-jobs-buried' => job_stats['current-jobs-buried'],
      'cmd-put' => @stats['cmd-put'],
      'cmd-peek' => @stats['cmd-peek'],
      'cmd-peek-ready' => @stats['cmd-peek-ready'],
      'cmd-peek-delayed' => @stats['cmd-peek-delayed'],
      'cmd-peek-buried' => @stats['cmd-peek-buried'],
      'cmd-reserve' => @stats['cmd-reserve'],
      'cmd-use' => @stats['cmd-use'],
      'cmd-watch' => @stats['cmd-watch'],
      'cmd-ignore' => @stats['cmd-ignore'],
      'cmd-delete' => @stats['cmd-delete'],
      'cmd-release' => @stats['cmd-release'],
      'cmd-bury' => @stats['cmd-bury'],
      'cmd-kick' => @stats['cmd-kick'],
      'cmd-stats' => @stats['cmd-stats'],
      'cmd-stats-job' => @stats['cmd-stats-job'],
      'cmd-stats-tube' => @stats['cmd-stats-tube'],
      'cmd-list-tubes' => @stats['cmd-list-tubes'],
      'cmd-list-tube-used' => @stats['cmd-list-tube-used'],
      'cmd-list-tubes-watched' => @stats['cmd-list-tubes-watched'],
      'cmd-pause-tube' => @stats['cmd-pause-tube'],
      'job-timeouts' => @stats['job-timeouts'],
      'total-jobs' => @jobs.total_jobs,
      'max-job-size' => @max_job_size,
      'current-tubes' => active_tubes.length,
      'current-connections' => connection_stats['current-connections'],
      'current-producers' => connection_stats['current-producers'],
      'current-workers' => connection_stats['current-workers'],
      'current-waiting' => connection_stats['current-waiting'],
      'total-connections' => @stats['total-connections'],
      'pid' => Process.pid,
      'version' => GemeraldBeanstalk::VERSION,
      'rusage-utime' =>'',
      'rusage-stime' =>'',
      'uptime' => (Time.now.to_f - @up_at).to_i,
      'binlog-oldest-index' => 0,
      'binlog-current-index' => 0,
      'binlog-records-written' => 0,
      'binlog-records-migrated' => 0,
      'binlog-max-size' => 10485760,
      'id' => @id,
      'hostname' => Socket.gethostname,
    }
    return yaml_response(stats.map{|stat, value| "#{stat}: #{value}" })
  end


  def stats_job(job_id)
    job = find_job(job_id)
    return NOT_FOUND if job.nil?

    return yaml_response(job.stats.map{ |stat, value| "#{stat}: #{value}" })
  end


  def stats_tube(tube_name)
    return NOT_FOUND if (tube = tube(tube_name)).nil?

    return yaml_response(tube.stats.map{ |stat, value| "#{stat}: #{value}" })
  end


  def touch(connection, job_id)
    job = find_job(job_id, :only => JOB_RESERVED_STATES)
    return NOT_FOUND if job.nil?

    job.touch(connection)
    return TOUCHED
  end


  def try_dispatch(connection, job)
    connection.mutex.synchronize do
      # Make sure connection still waiting and job not claimed
      return false unless connection.waiting? && job.reserve(connection)
      connection.transmit("RESERVED #{job.id} #{job.bytes}\r\n#{job.body}\r\n")
      cancel_reservations(connection)
    end
    @reserved[connection] << job
    return true
  end


  def tube(tube_name, create_if_missing = false)
    tube = @tubes[tube_name]

    return tube unless tube.nil? || tube.deactivated?

    return @tubes[tube_name] = GemeraldBeanstalk::Tube.new(tube_name) if create_if_missing

    @tubes.delete(tube_name) unless tube.nil?
    return nil
  end


  def tube_list(tube_list)
    return yaml_response(tube_list.map { |key| "- #{key}" })
  end


  def use(connection, tube_name)
    tube(tube_name, :create_if_missing).use
    connection.use(tube_name)

    return "USING #{tube_name}\r\n"
  end


  def valid_command?(command)
    return ALL_COMMANDS.include?(command)
  end


  def waiting_connections
    return @connections.select(&:waiting?)
  end


  def watch(connection, tube_name)
    tube(tube_name, :create_if_missing).watch
    watched_count = connection.watch(tube_name)

    return "WATCHING #{watched_count}\r\n"
  end


  def yaml_response(data)
    response = %w[---].concat(data).join("\n")
    return "OK #{response.bytesize}\r\n#{response}\r\n"
  end

  COMMAND_METHOD_PARAMETER_COUNTS = Hash[ALL_COMMANDS.map {|command| [command, instance_method(COMMAND_METHOD_NAMES[command]).parameters.length] }]

end
