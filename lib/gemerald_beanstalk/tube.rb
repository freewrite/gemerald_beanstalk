class GemeraldBeanstalk::Tube

  attr_reader :jobs, :name, :reservartions


  def active?
    return !self.deactivated?
  end


  def adjust_stats_key(key, adjustment = 1)
    @stats[key] = [@stats[key] + adjustment, 0].max
  end


  def cancel_reservation(connection)
    return @reservations.delete(connection)
  end


  def deactivate
    return false if @state == :deactivated || self.name == 'default'
    @state = :deactivated
    return true
  end


  def deactivated?
    return @state == :deactivated
  end


  def delete(job)
    adjust_stats_key(:'cmd-delete')
    return @jobs.delete(job)
  end


  def ignore
    adjust_stats_key(:'watching', -1)
    deactivate if should_deactivate?
  end


  def initialize(name)
    @name = name
    @jobs = GemeraldBeanstalk::Jobs.new
    @reservations = []
    @state = :ready
    @stats = ThreadSafe::Cache.new
    @stats[:'cmd-delete'] = 0
    @stats[:'cmd-pause-tube'] = 0
    @stats[:'using'] = 0
    @stats[:'waiting'] = 0
    @stats[:'watching'] = 0
  end


  def next_job(state = :ready, action = :reserve)
    return nil if paused? && action == :reserve

    best_candidate = nil
    @jobs.each do |candidate|
      next if candidate.state != state
      best_candidate = candidate if best_candidate.nil? || candidate < best_candidate
    end

    return best_candidate
  end


  def next_reservation
    reservation = nil
    while ready? && @reservations.any? && reservation.nil?
      reservation = @reservations[0]
      break if reservation.waiting?

      @reservations.shift
      reservation = nil
    end
    return reservation
  end


  def pause(delay, *args)
    return false unless ready?
    @state = :paused
    adjust_stats_key(:'cmd-pause-tube')
    @pause_delay = delay.to_i
    @paused_at = Time.now.to_f
    @resume_at = @paused_at + @pause_delay
    return true
  end


  def paused?
    return false unless @state == :paused
    return true if @resume_at > Time.now.to_f

    @state = :ready
    @pause_delay = @paused_at = @resume_at = nil
    return false
  end


  def put(job)
    @jobs.enqueue(job)
  end


  def ready?
    return @state == :ready
  end


  def reserve(connection)
    @reservations << connection
  end


  def should_deactivate?
    return @jobs.length == 0 && @stats[:'watching'] == 0 && @stats[:'using'] == 0
  end


  def stats
    job_stats = @jobs.counts_by_state
    # Need to call paused in advance to update state
    pause_time_left = paused? ? (@resume_at - Time.now.to_f).to_i : 0
    return {
      'name' => @name,
      'current-jobs-urgent' => job_stats['current-jobs-urgent'],
      'current-jobs-ready' => job_stats['current-jobs-ready'],
      'current-jobs-reserved' => job_stats['current-jobs-reserved'],
      'current-jobs-delayed' => job_stats['current-jobs-delayed'],
      'current-jobs-buried' => job_stats['current-jobs-buried'],
      'total-jobs' => @jobs.total_jobs,
      'current-using' => @stats[:'using'],
      'current-watching' => @stats[:'watching'],
      'current-waiting' => @reservations.length,
      'cmd-delete' => @stats[:'cmd-delete'],
      'cmd-pause-tube' => @stats[:'cmd-pause-tube'],
      'pause' => @pause_delay || 0,
      'pause-time-left' => pause_time_left,
    }
  end


  def stop_use
    adjust_stats_key(:'using', -1)
    deactivate if should_deactivate?
  end


  def watch
    adjust_stats_key(:'watching')
  end


  def use
    adjust_stats_key(:'using')
  end

end
