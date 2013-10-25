class GemeraldBeanstalk::Tube

  attr_reader :jobs, :name, :reservartions

  state_machine :state, :initial => :ready do
    event :pause do
      transition :ready => :paused
    end

    event :resume do
      transition :paused => :ready
    end

    event :deactivate do
      transition [:paused, :ready] => :deactivated unless self.name == 'default'
    end
  end


  def active?
    return !self.deactivated?
  end


  def adjust_stats_key(key, adjustment = 1)
    @mutex.synchronize do
      return @stats[key] += adjustment
    end
  end


  def cancel_reservation(connection)
    return @reservations.delete(connection)
  end


  def delete(job)
    adjust_stats_key('cmd-delete')
    return @jobs.delete(job)
  end


  def each(&block)
    return block_given? ? @jobs.each(&block) : @jobs.each
  end


  def ignore
    adjust_stats_key('watching', -1)
  end


  def initialize(name)
    @name = name
    @jobs = GemeraldBeanstalk::Jobs.new
    @mutex = Mutex.new
    @reservations = []
    @stats = {
      'cmd-delete' => 0,
      'cmd-pause-tube' => 0,
      'using' => 0,
      'waiting' => 0,
      'watching' => 0,
    }

    # Initialize state machine
    super()
  end


  def next_job(state = :ready, action = :reserve)
    return nil if paused? && action == :reserve

    best_candidate = nil
    @jobs.each do |candidate|
      next if candidate.state_name != state
      best_candidate = candidate if best_candidate.nil? || candidate < best_candidate
    end

    return best_candidate
  end


  def next_reservation
    reservation = nil
    while ready? && @reservations.any? && reservation.nil?
      reservation = @reservations[0]
      break if reservation.waiting?

      @mutex.synchronize do
        @reservations.shift
      end
      reservation = nil
    end
    return reservation
  end


  def pause(delay, *args)
    return false unless super
    adjust_stats_key('cmd-pause-tube')
    @pause_delay = delay.to_i
    @paused_at = Time.now.to_f
    @resume_at = @paused_at + @pause_delay
    return true
  end


  def paused?
    if self.state_name == :paused && @resume_at <= Time.now.to_f
      self.state = 'ready'
      @pause_delay = @paused_at = @resume_at = nil
    end
    super
  end


  def put(job)
    @mutex.synchronize do
      @jobs.enqueue(job)
    end
  end


  def reserve(connection)
    @reservations << connection
  end


  def stats
    job_stats = @jobs.counts_by_state
    # Need to call paused in advance to update state
    pause_time_left = paused? ? @resume_at - @paused_at : 0
    return {
      'name' => @name,
      'current-jobs-urgent' => job_stats['current-jobs-urgent'],
      'current-jobs-ready' => job_stats['current-jobs-ready'],
      'current-jobs-reserved' => job_stats['current-jobs-reserved'],
      'current-jobs-delayed' => job_stats['current-jobs-delayed'],
      'current-jobs-buried' => job_stats['current-jobs-buried'],
      'total-jobs' => @jobs.total_jobs,
      'current-using' => @stats['using'],
      'current-watching' => @stats['watching'],
      'current-waiting' => @stats['waiting'],
      'cmd-delete' => @stats['cmd-delete'],
      'cmd-pause-tube' => @stats['cmd-pause-tube'],
      'pause' => @pause_delay || 0,
      'pause-time-left' => pause_time_left,
    }
  end


  def stop_use
    adjust_stats_key('using', -1)
  end


  def watch
    adjust_stats_key('watching')
  end


  def use
    adjust_stats_key('using')
  end

end
