require 'eventmachine'
require 'debugger'

module GemeraldBeanstalk::Server

  def beanstalk
    return @beanstalk
  end

  def initialize(beanstalk)
    @beanstalk = beanstalk
    super
  end

  def post_init
    @connection = beanstalk.connect(self)
  end

  def receive_data(data)
    result = nil
    Thread.new do
      result = send_data(@connection.execute(data))
    end
    return result
  end

  def unbind
    beanstalk.disconnect(@connection)
    @connection.close_connection
  end

  def self.start(bind_address = '127.0.0.1', port = 11300)
    Thread.new do
      EventMachine.run do
        EventMachine.start_server(bind_address, port, GemeraldBeanstalk::Server, GemeraldBeanstalk::Beanstalk.new("#{bind_address}:#{port}"))
      end
    end
  end

end
