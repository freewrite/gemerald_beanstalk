module GemeraldBeanstalk::BeanstalkHelper

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


  def self.included(beanstalk)
    beanstalk.extend(ClassMethods)
  end


  module ClassMethods

    def load_plugin(plugin_name)
      include(GemeraldBeanstalk::Plugin.const_get(plugin_name))
    end

  end


  # ease handling of odd case where put can return BAD_FORMAT but increment stats
  def adjust_stats_cmd_put
    adjust_stats_key(:'cmd-put')
  end


  def connect(connection = nil)
    beanstalk_connection = GemeraldBeanstalk::Connection.new(self, connection)
    @connections << beanstalk_connection
    adjust_stats_key(:'total-connections')
    return beanstalk_connection
  end


  def disconnect(connection)
    connection.close_connection
    tube(connection.tube_used).stop_use
    connection.tubes_watched.each do |watched_tube|
      tube(watched_tube).ignore
      connection.ignore(watched_tube, :force)
    end
    @reserved[connection].each do |job|
      job.release(connection, job.priority, 0, false)
    end
    @reserved.delete(connection)
    @connections.delete(connection)
  end


  def execute(command)
    return send(command.method_name, *command.arguments)
  end


  def register_job_timeout(connection, job)
    @reserved[connection].delete(job)
    adjust_stats_key(:'job-timeouts')
    honor_reservations(job)
  end

  private

  def active_tubes
    tubes = {}
    @tubes.each_pair { |tube_name, tube| tubes[tube_name] = tube if tube.active? }
    return tubes
  end


  def adjust_stats_key(key, adjustment = 1)
    @stats[key] += adjustment
  end


  def cancel_reservations(connection)
    connection.tubes_watched.each do |tube_name|
      tube(tube_name).cancel_reservation(connection)
    end
    return connection
  end


  def deadline_pending?(connection)
    return @reserved[connection].any?(&:deadline_pending?)
  end


  def find_job(job_id, options = {})
    return unless (job_id = job_id.to_i) > 0
    only = Array(options[:only])
    except = Array(options[:except]).unshift(:deleted)

    job = @jobs[job_id - 1]

    return nil if job.nil? || except.include?(job.state)
    return (only.empty? || only.include?(job.state)) ? job : nil
  end


  def honor_reservations(job_or_tube)
    if job_or_tube.is_a?(GemeraldBeanstalk::Job)
      job = job_or_tube
      tube = tube(job.tube_name)
    elsif job_or_tube.is_a?(GemeraldBeanstalk::Tube)
      tube = job_or_tube
      job = tube.next_job
    end

    while job && (next_reservation = tube.next_reservation)
      next unless try_dispatch(next_reservation, job)
      job = tube.next_job
    end
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


  def peek_by_state(connection, state)
    adjust_stats_key(:"cmd-peek-#{state}")
    return peek_message(tube(connection.tube_used).next_job(state, :peek))
  end


  def peek_message(job)
    job.nil? ? NOT_FOUND : "FOUND #{job.id} #{job.bytes}\r\n#{job.body}\r\n"
  end


  def reserve_job(connection, timeout = 0)
    connection.worker = true

    if deadline_pending?(connection)
      connection.transmit(DEADLINE_SOON)
      return true
    end

    connection.tubes_watched.each do |tube_name|
      tube(tube_name).reserve(connection)
    end
    connection.wait(timeout <= 0 ? nil : Time.now.to_f + timeout)

    dispatched = false
    while !dispatched
      break if (job = next_job(connection)).nil?
      dispatched = try_dispatch(connection, job)
    end

    return dispatched
  end


  def stats_commands
    return {
      'cmd-put' => @stats[:'cmd-put'],
      'cmd-peek' => @stats[:'cmd-peek'],
      'cmd-peek-ready' => @stats[:'cmd-peek-ready'],
      'cmd-peek-delayed' => @stats[:'cmd-peek-delayed'],
      'cmd-peek-buried' => @stats[:'cmd-peek-buried'],
      'cmd-reserve' => @stats[:'cmd-reserve'],
      'cmd-reserve-with-timeout' => @stats[:'cmd-reserve-with-timeout'],
      'cmd-delete' => @stats[:'cmd-delete'],
      'cmd-release' => @stats[:'cmd-release'],
      'cmd-use' => @stats[:'cmd-use'],
      'cmd-watch' => @stats[:'cmd-watch'],
      'cmd-ignore' => @stats[:'cmd-ignore'],
      'cmd-bury' => @stats[:'cmd-bury'],
      'cmd-kick' => @stats[:'cmd-kick'],
      'cmd-touch' => @stats[:'cmd-touch'],
      'cmd-stats' => @stats[:'cmd-stats'],
      'cmd-stats-job' => @stats[:'cmd-stats-job'],
      'cmd-stats-tube' => @stats[:'cmd-stats-tube'],
      'cmd-list-tubes' => @stats[:'cmd-list-tubes'],
      'cmd-list-tube-used' => @stats[:'cmd-list-tube-used'],
      'cmd-list-tubes-watched' => @stats[:'cmd-list-tubes-watched'],
      'cmd-pause-tube' => @stats[:'cmd-pause-tube'],
    }
  end


  def stats_connections
    conn_stats = {
      'current-connections' => @connections.length,
      'current-producers' => 0,
      'current-workers' => 0,
      'current-waiting' => 0,
      'total-connections' => @stats[:'total-connections']
    }
    @connections.each do |connection|
      conn_stats['current-producers'] += 1 if connection.producer?
      conn_stats['current-waiting'] += 1 if connection.waiting?
      conn_stats['current-workers'] += 1 if connection.worker?
    end
    return conn_stats
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


  def update_state
    update_waiting
    update_timeouts
  end


  def update_timeouts
    @reserved.values.flatten.each(&:state)
    @delayed.keep_if do |job|
      case job.state
      when :delayed
        true
      when :ready
        honor_reservations(job)
        false
      else
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


  def update_waiting
    waiting_connections.each do |connection|
      if connection.waiting? && deadline_pending?(connection)
        message_for_connection = DEADLINE_SOON
      elsif connection.timed_out?
        message_for_connection = TIMED_OUT
      else
        next
      end

      cancel_reservations(connection)
      connection.transmit(message_for_connection)
    end
  end


  def uptime
    (Time.now.to_f - @up_at).to_i
  end


  def waiting_connections
    return @connections.select {|connection| connection.waiting? || connection.timed_out? }
  end


  def yaml_response(data)
    response = %w[---].concat(data).join("\n")
    return "OK #{response.bytesize}\r\n#{response}\r\n"
  end

end
