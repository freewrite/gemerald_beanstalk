module GemeraldBeanstalk::Plugin::Introspection

  def connections
    return @connections
  end


  def jobs
    return @jobs
  end


  def tubes
    return active_tubes
  end

end

GemeraldBeanstalk::Beanstalk.load_plugin(:Introspection)
