require 'eventmachine'

class GemeraldBeanstalk::Server

  # The default address to bind a server to. Matches beanstalkd.
  DEFAULT_BIND_ADDRESS = '0.0.0.0'

  # The default port that a server should listen on. Matches beanstalkd.
  DEFAULT_PORT = 11300

  # The address or hostname of the host the server is bound to
  attr_reader :bind_address

  # The port the server should listen on
  attr_reader :port

  # The bind_address and port of the server
  attr_reader :full_address

  # The beanstalk instance the server provides an interface for
  attr_reader :beanstalk

  # Index of existing servers
  @@servers = ThreadSafe::Cache.new


  # Returns the thread that the EventMachine event reactor is running in.
  #
  # @return [Thread] the thread the event reactor is running in.
  def self.event_reactor_thread
    return @@event_reactor_thread
  end


  # Create a new GemeraldBeanstalk::Server at the given `bind_address` and
  # `port`. `start_on_init` controls whether the server is immediately started
  # or starting the server should be deferred.
  #
  # @param bind_address [String] IP or hostname of the host the server is bound
  #   to
  # @param port [Integer, String] The port the server should listen on
  # @param start_on_init [Boolean] A boolean indicating whether or not the
  #   server should be started immediately or deferred.
  # @example Start a new server immediately at 0.0.0.0:11300
  #   GemeraldBeanstalk::Server.new
  # @example Create a new server at 127.0.0.1:11301 to be started later
  #   GemeraldBeanstalk::Server.new('127.0.0.1', 11301, false)
  def initialize(bind_address = nil, port = nil, start_on_init = true)
    @bind_address = bind_address || DEFAULT_BIND_ADDRESS
    @port = port.nil? ? DEFAULT_PORT : Integer(port)
    @full_address = "#{@bind_address}:#{@port}"
    @started = false
    start if start_on_init
  end


  # Flag indicating whether the server has been started and is currently
  # running
  def running?
    return @started
  end


  # Adds the server to the EventMachine reactor, effectively starting the
  # server. If the EventMachine reactor has not been started, it is started in
  # a new thread. In the process create a new GemeraldBeanstalk::Beanstalk that
  # the server provides the interface for. Returns after the server is open for
  # connections.
  #
  # Currently changes $PROGRAM_NAME, however this behavior is likely to change.
  #
  # @raise RuntimeError if a server is already registered at the server's full
  #   address
  # @return [GemeraldBeanstalk::Server] returns self
  def start
    raise RuntimeError, "Server already exists for address #{full_address}" if @@servers.put_if_absent(full_address, self)
    @beanstalk = GemeraldBeanstalk::Beanstalk.new(full_address)
    start_event_reactor
    EventMachine.run do
      @event_server = EventMachine.start_server(bind_address, port, GemeraldBeanstalk::EventServer, beanstalk)
      EventMachine.add_periodic_timer(0.01, beanstalk.method(:update_state))
    end
    $PROGRAM_NAME = "gemerald_beanstalk:#{full_address}"
    wait_for_action(:start)
    @started = true
    return self
  end


  # Stops the server by removing it from the EventMachine reactor. Returns when
  # the server is no longer available for connections.
  #
  # @raise RuntimeError if no server is registered at the server's full address.
  # @return [GemeraldBeanstalk::Server] returns self
  def stop
    registered_server = @@servers[full_address]
    raise "Server with address #{full_address} does not appear to have been started" unless registered_server
    EventMachine.stop_server(@event_server)
    wait_for_action(:stop)
    @@servers.delete(full_address)
    @started = false
    return self
  end


  private


  # Starts the EventMachine reactor in a new thread.
  def start_event_reactor
    return true if EventMachine.reactor_running?
    unless EventMachine.reactor_running?
      @@event_reactor_thread = Thread.new { EventMachine.run }
      while !EventMachine.reactor_running?
        sleep 0.1
      end
    end
    return true
  end


  # Handles waiting for a server instance to start or stop by repeatedly
  # attempting to open TCPSocket connections.
  def wait_for_action(action)
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

end
