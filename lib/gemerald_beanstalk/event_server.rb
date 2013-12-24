module GemeraldBeanstalk::EventServer

  def beanstalk
    return @beanstalk
  end


  def initialize(beanstalk)
    @beanstalk = beanstalk
    @partial_message = ''
    super
  end


  def post_init
    @connection = beanstalk.connect(self)
  end


  def receive_data(data)
    if data[-2, 2] == "\r\n"
      message = @partial_message + data
      @partial_message = ''
      EventMachine.defer(proc { @connection.execute(message) })
    else
      @partial_message += data
    end
  end


  def unbind
    beanstalk.disconnect(@connection)
    @connection.close_connection
  end

end
