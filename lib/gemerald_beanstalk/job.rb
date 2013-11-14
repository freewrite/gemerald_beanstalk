class GemeraldBeanstalk::Job

  MAX_JOB_PRIORITY = 2**32

  INACTIVE_STATES = [:buried, :delayed]
  RESERVED_STATES = [:deadline_pending, :reserved]
  UPDATE_STATES = [:deadline_pending, :delayed, :reserved]

  attr_reader :beanstalk, :reserved_at, :reserved_by, :timeout_at
  attr_accessor :priority, :tube_name, :delay, :ready_at, :body,
    :bytes, :created_at, :ttr, :id, :buried_at


  def <(other_job)
    return (self <=> other_job) == -1
  end


  def <=>(other_job)
    raise 'Cannot compare job with nil' if other_job.nil?
    current_state = state
    raise 'Cannot compare jobs with different states' if current_state != other_job.state

    case current_state
    when :ready
      return -1 if self.priority < other_job.priority ||
        self.priority == other_job.priority && self.created_at < other_job.created_at
    when :delayed
      return -1 if self.ready_at < other_job.ready_at
    when :buried
      return -1 if self.buried_at < other_job.buried_at
    else
      raise "Cannot compare job with state of #{current_state}"
    end
    return 1
  end


  def buried?
    return state == :buried
  end


  def bury(connection, priority, *args)
    return false unless reserved_by_connection?(connection)

    reset_reserve_state
    @state = :buried
    @stats_hash['buries'] += 1
    self.priority = priority.to_i
    self.buried_at = Time.now.to_f
    self.ready_at = nil
    return true
  end


  # Must look at @state to avoid infinite recursion
  def deadline_approaching(*args)
    return false unless @state == :reserved
    @state = :deadline_pending
    return true
  end


  def deadline_pending?
    return state == :deadline_pending
  end


  def delayed?
    return state == :delayed
  end


  def delete(connection, *args)
    return false if RESERVED_STATES.include?(state) && !reserved_by_connection?(connection)
    @state = :deleted
    return true
  end


  def initialize(beanstalk, id, tube_name, priority, delay, ttr, bytes, body)
    priority, delay, ttr = priority.to_i, delay.to_i, ttr.to_i
    @beanstalk = beanstalk
    @stats_hash = Hash.new(0)
    self.id = id
    self.tube_name = tube_name
    self.priority = priority % MAX_JOB_PRIORITY
    self.delay = delay
    self.ttr = ttr == 0 ? 1 : ttr
    self.bytes = bytes
    self.body = body
    self.created_at = Time.now.to_f
    self.ready_at = self.created_at + delay

    @state = delay > 0 ? :delayed : :ready
  end


  def kick(*args)
    return false unless INACTIVE_STATES.include?(state)

    @state = :ready
    @stats_hash['kicks'] += 1
    self.ready_at = Time.now.to_f
    self.buried_at = nil
    return true
  end


  def ready?
    return state == :ready
  end


  def release(connection, priority, delay, increment_stats = true, *args)
    return false unless reserved_by_connection?(connection)

    reset_reserve_state
    @state = delay > 0 ? :delayed : :ready
    @stats_hash['releases'] += 1 if increment_stats
    self.priority = priority.to_i
    self.delay = delay = delay.to_i
    self.ready_at = Time.now.to_f + delay
    return true
  end


  def reserve(connection, *args)
    return false unless ready?

    @state = :reserved
    @stats_hash['reserves'] += 1
    @reserved_by = connection
    @reserved_at = Time.now.to_f
    @timeout_at = @reserved_at + self.ttr
    return true
  end


  def reserved_by_connection?(connection)
    return RESERVED_STATES.include?(state) && self.reserved_by == connection ? true : false
  end


  def reset_reserve_state
    @timeout_at = nil
    @reserved_at = nil
    @reserved_by = nil
  end


  def state
    return @state unless UPDATE_STATES.include?(@state)

    now = Time.now.to_f
    if @state == :delayed && self.ready_at <= now
      @state = :ready
    elsif RESERVED_STATES.include?(@state)
      # Rescue from timeout being reset by other thread
      if (now > self.timeout_at rescue false)
        timed_out
      elsif (@state == :reserved && now + 1 > self.timeout_at rescue false)
        deadline_approaching
      end
    end

    return @state
  end


  def stats
    now = Time.now.to_f
    current_state = state
    if self.timeout_at
      time_left = (self.timeout_at - now).to_i
    elsif self.ready_at
      time_left = (self.ready_at - now).to_i
    end
    return {
      'id' => self.id,
      'tube' => self.tube_name,
      'state' => current_state == :deadline_pending ? 'reserved' : current_state.to_s,
      'pri' => self.priority,
      'age' => (now - self.created_at).to_i,
      'delay' => self.delay || 0,
      'ttr' => self.ttr,
      'time-left' => time_left || 0,
      'file' => 0,
      'reserves' => @stats_hash['reserves'],
      'timeouts' => @stats_hash['timeouts'],
      'releases' => @stats_hash['releases'],
      'buries' => @stats_hash['buries'],
      'kicks' => @stats_hash['kicks'],
    }
  end


  # Must reference @state to avoid infinite recursion
  def timed_out(*args)
    return false unless RESERVED_STATES.include?(@state)
    @state = :ready
    @stats_hash['timeouts'] += 1
    connection = self.reserved_by
    reset_reserve_state
    self.beanstalk.register_job_timeout(connection, self)
    return true
  end


  def timed_out?
    return state == :timed_out
  end


  def touch(connection)
    return false unless reserved_by_connection?(connection)
    @state = :reserved
    @timeout_at = Time.now.to_f + self.ttr
    return true
  end

end
