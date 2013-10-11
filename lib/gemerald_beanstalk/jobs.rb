class GemeraldBeanstalk::Jobs < Array
  attr_reader :total_jobs

  def counts_by_state
    job_stats = Hash.new(0)
    self.each do |job|
      state = job.state_name
      next if state == :deleted

      job_stats["current-jobs-#{state}"] += 1
      job_stats['current-jobs-urgent'] += 1 if state == :ready && job.priority < 1024
    end
    return job_stats
  end


  def enqueue(job)
    @total_jobs += 1
    push(job)
  end


  def initialize(*)
    @total_jobs = 0
    super
  end

end
