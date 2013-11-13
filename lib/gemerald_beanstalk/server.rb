require 'eventmachine'

module GemeraldBeanstalk::Server

  def self.start(bind_address = nil, port = nil)
    bind_address ||= '0.0.0.0'
    port ||= 11300
    full_address = "#{bind_address}:#{port}"
    beanstalk = GemeraldBeanstalk::Beanstalk.new(full_address)
    thread = Thread.new do
      EventMachine.run do
        EventMachine.start_server(bind_address, port, GemeraldBeanstalk::Server, beanstalk)
        EventMachine.add_periodic_timer(0.01, beanstalk.method(:update_state))
      end
    end
    $PROGRAM_NAME = "gemerald_beanstalk:#{full_address}"
    return [thread, beanstalk]
  end


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
