require 'securerandom'
require 'socket'
require 'gemerald_beanstalk/beanstalk_helper'

class GemeraldBeanstalk::Beanstalk

  include GemeraldBeanstalk::BeanstalkHelper

  attr_reader :address, :max_job_size


  def initialize(address, maximum_job_size = 2**16)
    @max_job_size = maximum_job_size
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


  protected

  def bury(connection, job_id, priority, *args)
    adjust_stats_key(:'cmd-bury')
    job = find_job(job_id, :only => JOB_RESERVED_STATES)
    return NOT_FOUND if job.nil? || !job.bury(connection, priority)

    @reserved[connection].delete(job)
    return BURIED
  end


  def delete(connection, job_id = nil, *args)
    adjust_stats_key(:'cmd-delete')
    job_id = job_id.to_i
    job = find_job(job_id)
    return NOT_FOUND if job.nil?

    original_state = job.state
    return NOT_FOUND unless job.delete(connection)

    tube(job.tube_name).delete(job)
    @jobs[job.id - 1] = nil
    @reserved[connection].delete(job) if JOB_RESERVED_STATES.include?(original_state)

    return DELETED
  end


  def ignore(connection, tube_name)
    adjust_stats_key(:'cmd-ignore')
    return NOT_IGNORED if (watched_count = connection.ignore(tube_name)).nil?
    tube = tube(tube_name)
    tube.ignore unless tube.nil?
    return "WATCHING #{watched_count}\r\n"
  end


  def kick(connection, limit, *args)
    adjust_stats_key(:'cmd-kick')
    limit = limit.to_i
    kicked = 0
    JOB_INACTIVE_STATES.each do |job_state|
      # GTE to handle negative limits
      break if kicked >= limit
      until (job = tube(connection.tube_used).next_job(job_state, :peek)).nil?
        kicked += 1 if job.kick
        break if kicked == limit
      end
      break if kicked > 0
    end

    return "KICKED #{kicked}\r\n"
  end


  def kick_job(connection, job_id = nil, *args)
    job_id = job_id.to_i
    job = find_job(job_id, :only => JOB_INACTIVE_STATES)
    return (!job.nil? && job.kick) ? KICKED : NOT_FOUND
  end


  def list_tubes(connection)
    adjust_stats_key(:'cmd-list-tubes')
    return tube_list(active_tubes.keys)
  end


  def list_tube_used(connection)
    adjust_stats_key(:'cmd-list-tube-used')
    return "USING #{connection.tube_used}\r\n"
  end


  def list_tubes_watched(connection)
    adjust_stats_key(:'cmd-list-tubes-watched')
    return tube_list(connection.tubes_watched)
  end


  def pause_tube(connection, tube_name, delay)
    adjust_stats_key(:'cmd-paue-tube')
    return NOT_FOUND if (tube = tube(tube_name)).nil?
    tube.pause(delay.to_i % 2**32)
    @paused << tube
    return PAUSED
  end


  def peek(connection, job_id = nil, *args)
    adjust_stats_key(:'cmd-peek')
    return peek_message(job_id.to_i > 0 ? find_job(job_id) : nil)
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
    adjust_stats_key(:'cmd-put')
    bytes = bytes.to_i
    return JOB_TOO_BIG if bytes > @max_job_size
    return EXPECTED_CRLF if body.slice!(-2, 2) != CRLF || body.length != bytes

    job = nil
    # Ensure job insertion order and ID
    @mutex.synchronize do
      job = GemeraldBeanstalk::Job.new(self, @jobs.next_id, connection.tube_used, priority, delay, ttr, bytes, body)
      @jobs.enqueue(job)
      tube(connection.tube_used).put(job)
    end

    # Send async so client doesn't wait while we check if job can be immediately dispatched
    connection.transmit("INSERTED #{job.id}\r\n")

    connection.producer = true

    case job.state
    when :ready
      honor_reservations(job)
    when :delayed
      @delayed << job
    end
    return nil
  end


  def quit(connection)
    disconnect(connection)
    return nil
  end


  def release(connection, job_id, priority, delay)
    adjust_stats_key(:'cmd-release')
    job = find_job(job_id, :only => JOB_RESERVED_STATES)
    return NOT_FOUND if job.nil? || !job.release(connection, priority.to_i, delay.to_i)

    @reserved[connection].delete(job)
    @delayed << job if job.delayed?
    return RELEASED
  end


  def reserve(connection, *args)
    adjust_stats_key(:'cmd-reserve')
    return BAD_FORMAT unless args.empty?
    reserve_job(connection)
    return nil
  end


  def reserve_with_timeout(connection, timeout = 0, *args)
    adjust_stats_key(:'cmd-reserve-with-timeout')
    timeout = timeout.to_i
    return nil if reserve_job(connection, timeout) || timeout != 0
    connection.wait_timed_out
    return TIMED_OUT
  end


  def stats(connection)
    adjust_stats_key(:'cmd-stats')
    stats = @jobs.counts_by_state.merge(stats_commands).merge({
      'job-timeouts' => @stats[:'job-timeouts'],
      'total-jobs' => @jobs.total_jobs,
      'max-job-size' => @max_job_size,
      'current-tubes' => active_tubes.length,
    }).merge(stats_connections).merge({
      'pid' => Process.pid,
      'version' => GemeraldBeanstalk::VERSION,
      'rusage-utime' => 0,
      'rusage-stime' => 0,
      'uptime' => uptime,
      'binlog-oldest-index' => 0,
      'binlog-current-index' => 0,
      'binlog-records-migrated' => 0,
      'binlog-records-written' => 0,
      'binlog-max-size' => 10485760,
      'id' => @id,
      'hostname' => Socket.gethostname,
    })
    return yaml_response(stats.map{|stat, value| "#{stat}: #{value}" })
  end


  def stats_job(connection, job_id = nil, *args)
    adjust_stats_key(:'cmd-stats-job')
    job_id = job_id.to_i
    job = find_job(job_id)
    return NOT_FOUND if job.nil?

    return yaml_response(job.stats.map{ |stat, value| "#{stat}: #{value}" })
  end


  def stats_tube(connection, tube_name)
    adjust_stats_key(:'cmd-stats-tube')
    return NOT_FOUND if (tube = tube(tube_name)).nil?

    return yaml_response(tube.stats.map{ |stat, value| "#{stat}: #{value}" })
  end


  def touch(connection, job_id = nil, *args)
    adjust_stats_key(:'cmd-touch')
    job_id = job_id.to_i
    job = find_job(job_id, :only => JOB_RESERVED_STATES)
    return NOT_FOUND if job.nil? || !job.touch(connection)

    return TOUCHED
  end


  def use(connection, tube_name)
    adjust_stats_key(:'cmd-use')
    tube(connection.tube_used).stop_use
    tube(tube_name, :create_if_missing).use
    connection.use(tube_name)

    return "USING #{tube_name}\r\n"
  end


  def watch(connection, tube_name)
    adjust_stats_key(:'cmd-watch')
    if connection.tubes_watched.include?(tube_name)
      watched_count = connection.tubes_watched.length
    else
      tube(tube_name, :create_if_missing).watch
      watched_count = connection.watch(tube_name)
    end

    return "WATCHING #{watched_count}\r\n"
  end

end
