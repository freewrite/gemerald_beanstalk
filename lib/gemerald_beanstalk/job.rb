require 'debugger'
class GemeraldBeanstalk::Job

  # releases is not included because it is conditionally incremented
  STATS_KEYS = {:bury => 'buries', :kick => 'kicks', :reserve => 'reserves'}

  INACTIVE_STATES = [:buried, :delayed]
  RESERVED_STATES = [:deadline_pending, :reserved]
  UPDATE_STATES = [:deadline_pending, :delayed, :reserved]

  attr_reader :beanstalk, :reserved_at, :reserved_by, :timeout_at
  attr_accessor :priority, :tube_name, :delay, :ready_at, :body,
    :bytes, :created_at, :ttr, :id, :stats_hash, :buried_at


  state_machine :state do
    event :bury do
      transition RESERVED_STATES => :buried
    end


    event :reserve do
      transition :ready => :reserved
    end


    event :delete do
      transition any => :deleted
    end


    event :kick do
      transition INACTIVE_STATES => :ready
    end


    event :deadline_approaching do
      # See #deadline_approaching
    end


    event :release do
      # See #release
    end

    event :timed_out do
      # See #timed_out
    end


    event :touch do
      transition RESERVED_STATES => :reserved
    end


    around_transition do |job, transition, block|
      stats_key = STATS_KEYS[transition.event]
      unless stats_key.nil?
        job.stats_hash[stats_key] += 1
      end
      block.call
    end

  end


  def <(other_job)
    return (self <=> other_job) == -1
  end


  def <=>(other_job)
    raise 'Cannot compare job with nil' if other_job.nil?
    raise 'Cannot compare jobs with different states' if self.state_name != other_job.state_name

    if self.state_name == :ready
      return -1 if self.priority < other_job.priority ||
        self.priority == other_job.priority && self.created_at < other_job.created_at
    elsif self.state_name == :delayed
      return -1 if self.ready_at < other_job.ready_at
    elsif self.state_name == :buried
      return -1 if self.buried_at < other_job.buried_at
    else
      raise "Cannot compare job with state of #{self.state}"
    end
    return 1
  end


  def bury(connection, priority, *args)
    return false unless reserved_by_connection?(connection) && super

    reset_reserve_state
    self.priority = priority.to_i
    self.buried_at = Time.now.to_f
    self.ready_at = nil
    return true
  end


  # Needs override to work with #update_state
  def deadline_approaching(*args)
    return false unless @state == 'reserved'
    @state = 'deadline_pending'
  end


  def delete(connection, *args)
    return false if RESERVED_STATES.include?(self.state_name) && !reserved_by_connection?(connection)
    return super
  end


  def initialize(beanstalk, id, tube_name, priority, delay, ttr, bytes, body)
    @beanstalk = beanstalk
    self.id = id
    self.tube_name = tube_name
    self.priority = priority.to_i
    self.delay = delay = delay.to_i
    self.ttr = ttr.to_i
    self.bytes = bytes.to_i
    self.body = body
    self.stats_hash = Hash.new(0)
    self.created_at = Time.now.to_f
    self.ready_at = self.created_at + delay

    # Initilize state machine
    @state = delay > 0 ? 'delayed' : 'ready'
    super()
  end


  def kick(*args)
    return false unless super

    self.state = :ready
    self.ready_at = Time.now.to_f
    self.buried_at = nil
    return true
  end


  def release(connection, priority, delay, increment_stats = true, *args)
    return false unless reserved_by_connection?(connection)

    reset_reserve_state
    self.state = delay > 0 ? 'delayed' : 'ready'
    self.stats_hash['releases'] += 1 if increment_stats
    self.priority = priority.to_i
    self.delay = delay = delay.to_i
    self.ready_at = Time.now.to_f + delay
    return true
  end


  def reserve(connection, *args)
    return false unless super

    @reserved_by = connection
    @reserved_at = Time.now.to_f
    @timeout_at = @reserved_at + self.ttr
    return true
  end


  def reserved_by_connection?(connection)
    return RESERVED_STATES.include?(self.state_name) && self.reserved_by == connection ? true : false
  end


  def reset_reserve_state
    @timeout_at = nil
    @reserved_at = nil
    @reserved_by = nil
  end


  def state
    update_state
    return super
  end


  def state_name
    update_state
    return super
  end


  def stats
    now = Time.now.to_f
    return {
      'id' => self.id,
      'tube' => self.tube_name,
      'state' => self.state_name == :deadline_pending ? 'reserved' : self.state,
      'pri' => self.priority,
      'age' => (now - self.created_at).to_i,
      'delay' => self.delay || 0,
      'ttr' => self.ttr,
      'time-left' => self.timeout_at ? (self.timeout_at - now).to_i : 0,
      'file' => 0,
      'reserves' => self.stats_hash['reserves'],
      'timeouts' => self.stats_hash['timeouts'],
      'releases' => self.stats_hash['releases'],
      'buries' => self.stats_hash['buries'],
      'kicks' => self.stats_hash['kicks'],
    }
  end


  def timed_out(*args)
    return false unless RESERVED_STATES.include?(@state.to_sym)
    self.state = 'ready'
    connection = self.reserved_by
    reset_reserve_state
    self.beanstalk.register_job_timeout(connection, self)
    return true
  end


  def touch(connection)
    return unless reserved_by_connection?(connection) && super
    @timeout_at = Time.now.to_f + self.ttr
  end


  def update_state
    state_sym = @state.to_sym
    return unless UPDATE_STATES.include?(state_sym)

    now = Time.now.to_f
    if state_sym == :delayed && self.ready_at <= now
      @state = 'ready'
    elsif RESERVED_STATES.include?(state_sym)
      if now > self.timeout_at
        timed_out
      elsif state_sym == :reserved && now + 1 > self.timeout_at
        deadline_approaching
      end
    end
  end

end
