require 'test_helper'

class ServerTest < GemeraldBeanstalkTest

  context '#new' do

    should 'use defaults if specific values are not provided' do
      default_bind_address = GemeraldBeanstalk::Server::DEFAULT_BIND_ADDRESS
      default_port = GemeraldBeanstalk::Server::DEFAULT_PORT
      GemeraldBeanstalk::Server.any_instance.expects(:start)
      server = GemeraldBeanstalk::Server.new
      assert_equal default_bind_address, server.bind_address
      assert_equal default_port, server.port
      assert_equal "#{default_bind_address}:#{default_port}", server.full_address
    end


    should 'take a bind_address, port, and auto start value' do
      GemeraldBeanstalk::Server.any_instance.expects(:start).never
      bind_address = '127.0.0.1'
      port = 11301
      server = GemeraldBeanstalk::Server.new(bind_address, port, false)
      assert_equal bind_address, server.bind_address
      assert_equal port, server.port
      assert_equal "#{bind_address}:#{port}", server.full_address
      assert_equal false, server.running?
    end


    should 'raise ArgumentError if port is not a valid integer' do
      assert_raises(ArgumentError) do
        GemeraldBeanstalk::Server.new('0.0.0.0', 'xxx', false)
      end
    end

  end


  context '#start' do

    setup do
      @bind_address = '0.0.0.0'
      @port = 11400
      @full_address = "#{@bind_address}:#{@port}"
      @server = GemeraldBeanstalk::Server.new(@bind_address, @port, false)
    end


    teardown do
      @server.stop if @server.running?
    end


    should 'raise RuntimeError if a server is already registered at given address' do
      @server.start
      assert_raises(RuntimeError) do
        GemeraldBeanstalk::Server.new(@bind_address, @port)
      end
    end


    should 'create a beanstalk with correct address for the server' do
      beanstalk = GemeraldBeanstalk::Beanstalk.new(@full_address)
      GemeraldBeanstalk::Beanstalk.expects(:new).with(@full_address).returns(beanstalk)
      @server.start
    end


    should 'wait for server to start and set started to true' do
      @server.start
      begin
        socket = TCPSocket.new(@bind_address, @port)
        socket.close
        started = true
      rescue
        started = false
      end
      assert_equal true, @server.running?
      assert started, 'Expected to be able to open TCP connection to started server'
    end


    should 'register the server' do
      @server.start
      assert_equal GemeraldBeanstalk::Server.class_variable_get(:@@servers)[@full_address], @server
    end

  end


  context '#stop' do

    setup do
      @bind_address = '0.0.0.0'
      @port = 11400
      @full_address = "#{@bind_address}:#{@port}"
      @server = GemeraldBeanstalk::Server.new(@bind_address, @port, false)
    end


    should 'raise an error if the server is not registered' do
      assert_raises(RuntimeError) do
        @server.stop
      end
      @server.start
      @server.stop
      assert_equal false, @server.running?
      assert_raises(RuntimeError) do
        @server.stop
      end
    end


    should 'kill the server and wait for its death' do
      @server.start
      @server.stop
      assert_equal false, @server.running?
      begin
        socket = TCPSocket.new(@bind_address, @port)
        socket.close
        stopped = false
      rescue
        stopped = true
      end
      assert stopped, 'Did not expect to be able to open TCP connection to stopped server'
    end

  end


  context '#start_event_loop' do

    setup do
      @server = GemeraldBeanstalk::Server.new(nil, nil, false)
    end


    should 'start the EventMachine reactor' do
      @server.send(:start_event_loop)
      assert EventMachine.reactor_running?
    end


    should 'not try to start EventMachine reactor if already running' do
      @server.send(:start_event_loop)
      Thread.expects(:new).never
      @server.send(:start_event_loop)
    end

  end

end
