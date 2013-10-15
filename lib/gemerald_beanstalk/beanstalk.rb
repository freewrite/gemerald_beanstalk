require 'securerandom'
require 'set'
require 'socket'

class GemeraldBeanstalk::Beanstalk
  COMMANDS = %w[bury delete ignore kick kick-job list-tubes list-tube-used
    list-tubes-watched pause-tube peek peek-buried peek-delayed peek-ready put quit release
    reserve reserve-with-timeout stats stats-job stats-tube touch use watch]

  COMMAND_METHOD_NAMES = Hash[COMMANDS.zip(COMMANDS)].merge!({'kick-job' => :kick_job, 'list-tubes' => :list_tubes, 'list-tube-used' => :list_tube_used,
    'list-tubes-watched' => :list_tubes_watched, 'pause-tube' => :pause_tube, 'peek-buried' => :peek_buried,
    'peek-delayed' => :peek_delayed, 'peek-ready' => :peek_ready, 'reserve-with-timeout' => :reserve_with_timeout,
    'stats-job' => :stats_job, 'stats-tube' => :stats_tube})

  CONNECTION_SPECIFIC_COMMANDS = %w[bury delete ignore kick list-tube-used list-tubes-watched
    peek-buried peek-delayed peek-ready put quit release reserve reserve-with-timeout touch use watch]

  STATS_COMMANDS = %w[bury delete ignore kick list-tube-used list-tubes list-tubes-watched pause-tube peek
    peek-buried peek-delayed peek-ready put release reserve stats stats-job stats-tube use watch]

  attr_reader :address, :tubes, :connections

  def connect(tcp_connection = nil)
    connection = GemeraldBeanstalk::Connection.new(self, tcp_connection)
    @connections << connection
    adjust_stats_key('total-connections')
    return connection
  end


  def disconnect(connection)
    tube(connection.tube_used).stop_use
    @reserved[connection].each do |job|
      job.release(connection, job.priority, 0, false)
    end
    @reserved.delete(connection)
    connections.delete(connection)
    connection.close_connection
  end


  def execute(connection, command = nil, *command_params)
    return unknown_command! unless valid_command?(command)

    adjust_stats_key("cmd-#{command}") if STATS_COMMANDS.include?(command)

    command_params.unshift(connection) if connection_specific_command?(command)

    return bad_format! unless COMMAND_METHOD_PARAMETER_COUNTS[command] == command_params.length

    return send(COMMAND_METHOD_NAMES[command], *command_params)
  end


  def initialize(address, max_job_size = 65535)
    @max_job_size = max_job_size
    @address = address
    @connections = []
    @id = SecureRandom.base64(16)
    @jobs = GemeraldBeanstalk::Jobs.new
    @mutex = Mutex.new
    @reserved = Hash.new([])
    @stats = Hash.new(0)
    @tubes = {}
    @up_at = Time.now.to_f

    tube('default', :create_if_missing)
  end


  def register_job_timeout
    adjust_stats_key('job-timeouts')
  end

  protected

  def active_tubes
    return @tubes.select { |tube_name, tube| tube.active? }
  end


  def adjust_stats_key(key, adjustment = 1)
    @mutex.synchronize do
      @stats[key] += adjustment
    end
  end


  def bad_format!
    return "BAD_FORMAT\r\n"
  end


  def bury(connection, job_id, priority)
    job = find_job(job_id, :only => :reserved)
    return not_found! if job.nil? || !job.bury(connection, priority)

    @reserved[connection].delete(job)
    return "BURIED\r\n"
  end


  def connection_specific_command?(command)
    return CONNECTION_SPECIFIC_COMMANDS.include?(command)
  end


  def deadline_soon!
    return "DEADLINE_SOON\r\n"
  end


  def delete(connection, job_id)
    job = find_job(job_id)
    return not_found! if job.nil? || !job.delete(connection)

    @mutex.synchronize do
      tube(job.tube_name).delete(job)
    end

    return "DELETED\r\n"
  end


  def ignore(connection, tube_name)
    return "NOT_IGNORED\r\n" if connection.tubes_watched.length == 1

    connection.ignore(tube_name)
    tube(tube_name).ignore

    return "WATCHING #{connection.tubes_watched.length}\r\n"
  end


  def find_job(job_id, options = {})
    only = Array(options[:only])
    except = Array(options[:except]).map(&:to_sym).unshift(:deleted)

    job = @jobs[job_id.to_i - 1]

    return nil if job.nil? || except.include?(job.state_name)
    return (only.empty? || only.include?(job.state_name)) ? job : nil
  end


  def kick(connection, limit)
    limit = limit.to_i
    kicked = 0
    [:buried, :delayed].each do |job_state|
      break if kicked == limit
      until (job = tube(connection.tube_used).next_job(job_state, :peek)).nil?
        kicked += 1 if job.kick
      end
    end

    return "KICKED #{kicked}\r\n"
  end


  def kick_job(job_id)
    job = find_job(job_id, :only => [:buried, :delayed])
    return not_found! if job.nil?

    return job.kick ? "KICKED\r\n" : not_found!
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


  def not_found!
    return "NOT_FOUND\r\n"
  end


  def pause_tube(tube_name, delay)
    return not_found! unless tube = tube(tube_name)

    tube.pause(delay)
    return "PAUSED\r\n"
  end


  def peek(job_id)
    job = find_job(job_id)
    return job.nil? ? not_found! : "FOUND #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
  end


  def peek_by_state(connection, state)
    job = tube(connection.tube_used).next_job(state, :peek)
    return job.nil? ? not_found! : "FOUND #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
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
    return "JOB_TOO_BIG\r\n" if bytes.to_i > @max_job_size
    return "EXPECTED_CRLF\r\n" if body.length - 2 != bytes.to_i || (body = body.gsub!(/\r\n$/, '')).nil?

    id = nil
    @mutex.synchronize do
      id = @jobs.total_jobs + 1
      job = GemeraldBeanstalk::Job.new(self, id, connection.tube_used, priority, delay, ttr, bytes, body)
      @jobs.enqueue(job)
      tube(connection.tube_used).put(job)
    end
    connection.producer = true unless connection.producer?

    return "INSERTED #{id}\r\n"
  end


  def quit(connection)
    disconnect(connection)
    return nil
  end


  def release(connection, job_id, priority, delay)
    job = find_job(job_id)
    return not_found! if job.nil?

    delay = delay.to_i
    success = job.release(connection, priority.to_i, delay.to_i)

    return bad_format! unless success

    @reserved[connection].delete(job)
    return "RELEASED\r\n"
  end


  def reserve(connection)
    return reserve_job(connection)
  end


  def reserve_job(connection, timeout = nil)
    job = nil
    reserved = false
    timeout_at = Time.now.to_f + timeout unless [nil, 0].include?(timeout)
    connection.worker = true unless connection.worker?
    connection.waiting = true
    while [nil, 0].include?(timeout) || Time.now.to_f < timeout_at
      @reserved[connection].each(&:state)
      return deadline_soon! if connection.deadline_pending?

      job = next_job(connection)
      timeout == 0 ? break : next if job.nil?

      @mutex.synchronize do
        # Make sure another thread hasn't already cliamed this job
        reserved = job.reserve(connection)
      end
      break if reserved || timeout == 0
    end
    connection.waiting = false

    return nil if job.nil?

    @reserved[connection] << job
    return "RESERVED #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
  end



  def reserve_with_timeout(connection, timeout)
    job_message = reserve_job(connection, timeout.to_i)
    return job_message.nil? ? "TIMED_OUT\r\n" : job_message
  end


  def stats
    job_stats = @jobs.counts_by_state
    connection_stats = {
      'current-producers' => 0,
      'current-waiting' => 0,
      'current-workers' => 0,
    }
    connections.each do |connection|
      connection_stats['current-producers'] += 1 if connection.producer?
      connection_stats['current-waiting'] += 1 if connection.waiting?
      connection_stats['current-workers'] += 1 if connection.worker?
    end
    connection_stats['current-connections'] = connections.length
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
    return not_found! if job.nil?

    statss = job.stats
    return yaml_response(statss.map{ |stat, value| "#{stat}: #{value}" })
  end


  def stats_tube(tube_name)
    return not_found! unless tube = tube(tube_name)

    return yaml_response(tube.stats.map{ |stat, value| "#{stat}: #{value}" })
  end


  def touch(connection, job_id)
    job = find_job(job_id, :only => :reserved)
    return not_found! if job.nil?

    job.touch(connection)
    return "TOUCHED\r\n"
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


  def tubes
    return active_tubes
  end


  def unknown_command!
    return "UNKNOWN_COMMAND\r\n"
  end


  def use(connection, tube_name)
    tube(tube_name, :create_if_missing).use
    connection.use(tube_name)

    return "USING #{tube_name}\r\n"
  end


  def valid_command?(command)
    return COMMANDS.include?(command)
  end


  def watch(connection, tube_name)
    tube(tube_name, :create_if_missing).watch
    connection.watch(tube_name)

    return "WATCHING #{connection.tubes_watched.length}\r\n"
  end


  def yaml_response(data)
    response = %w[---].concat(data).join("\n")
    return "OK #{response.bytesize}\r\n#{response}\r\n"
  end

  COMMAND_METHOD_PARAMETER_COUNTS = Hash[COMMANDS.map {|command| [command, instance_method(COMMAND_METHOD_NAMES[command]).parameters.length] }]

end
