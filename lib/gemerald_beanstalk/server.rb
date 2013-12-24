require 'eventmachine'

module GemeraldBeanstalk::Server

  @@servers = ThreadSafe::Cache.new


  def self.address_components(bind_address = nil, port = nil)
    bind_address ||= '0.0.0.0'
    port = port.nil? ? 11300 : Integer(port)
    return {
      :bind_address => bind_address,
      :full_address => "#{bind_address}:#{port}",
      :port => port,
    }
  end


  def self.start(bind_address = nil, port = nil)
    components = address_components(bind_address, port)
    raise RuntimeError, "Server already exists for address #{components[:full_address]}" if @@servers.key?(components[:full_address])
    beanstalk = GemeraldBeanstalk::Beanstalk.new(components[:full_address])
    server_thread = Thread.new do
      EventMachine.run do
        EventMachine.start_server(components[:bind_address], components[:port], GemeraldBeanstalk::Server, beanstalk)
        EventMachine.add_periodic_timer(0.01, beanstalk.method(:update_state))
      end
    end
    $PROGRAM_NAME = "gemerald_beanstalk:#{components[:full_address]}"
    thread_and_beanstalk = [server_thread, beanstalk]
    wait_for_server(:start, components[:bind_address], components[:port])
    @@servers[components[:full_address]] = thread_and_beanstalk
    return thread_and_beanstalk
  end


  def self.stop(bind_address = nil, port = nil)
    components = address_components(bind_address, port)
    serv = @@servers.delete(components[:full_address])
    raise "No server found with address #{components[:full_address]}" if serv.nil?
    serv[0].kill
    wait_for_server(:stop, components[:bind_address], components[:port])
  end


  def self.wait_for_server(action, bind_address, port)
    action = action.to_sym
    loop do
      begin
        TCPSocket.new(bind_address, port)
      rescue Errno::ECONNREFUSED
        next if action == :start
        break if action == :stop
      rescue Errno::ECONNRESET
        break if action == :stop
      end
      break if action == :start
    end
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
