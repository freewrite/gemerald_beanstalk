class GemeraldBeanstalk::Jobs < ThreadSafe::Array
  attr_reader :total_jobs

  def counts_by_state
    job_stats = Hash.new(0)
    self.compact.each do |job|
      state = job.state

      job_stats["current-jobs-#{state}"] += 1
      job_stats['current-jobs-urgent'] += 1 if state == :ready && job.priority < 1024
    end
    return job_stats
  end


  def enqueue_existing(job)
    @total_jobs += 1
    push(job)
    return job
  end


  def enqueue_new(beanstalk, id, tube_used, priority, delay, ttr, bytes, body)
    return enqueue_existing(
      GemeraldBeanstalk::Job.new(beanstalk, id, tube_used, priority, delay, ttr, bytes, body)
    )
  end


  def initialize(*)
    @total_jobs = 0
    super
  end

end
