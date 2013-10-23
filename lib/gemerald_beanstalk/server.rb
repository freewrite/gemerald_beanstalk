require 'eventmachine'

module GemeraldBeanstalk::Server

  def self.start(bind_address = nil, port = nil)
    bind_address ||= '0.0.0.0'
    port ||= 11300
    beanstalk = GemeraldBeanstalk::Beanstalk.new("#{bind_address}:#{port}")
    thread = Thread.new do
      EventMachine.run do
        EventMachine.start_server(bind_address, port, GemeraldBeanstalk::Server, beanstalk)
        EventMachine.add_periodic_timer(0.01, beanstalk.method(:update_waiting_connections))
      end
    end
    return [thread, beanstalk]
  end


  def beanstalk
    return @beanstalk
  end


  def initialize(beanstalk)
    @send_data_callback = proc {|response| send_data(response) unless response.nil? }
    @beanstalk = beanstalk
    super
  end


  def post_init
    @connection = beanstalk.connect(self)
  end


  def receive_data(data)
    return if @connection.waiting?
    EventMachine.defer(proc { @connection.execute(data) }, @send_data_callback)
  end


  def unbind
    beanstalk.disconnect(@connection)
    @connection.close_connection
  end

end
