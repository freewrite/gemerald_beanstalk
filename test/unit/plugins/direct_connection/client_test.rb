require 'test_helper'
require 'gemerald_beanstalk/plugins/direct_connection'

class DirectConnectionClientTest < GemeraldBeanstalkTest

  setup do
    @beanstalk = GemeraldBeanstalk::Beanstalk.new('localhost:11300')
    @beanstalk.extend(GemeraldBeanstalk::Plugin::DirectConnection)
  end


  context '#transmit' do

    should 'connect directly to Gemerald server' do
      GemeraldBeanstalk::EventServer.expects(:new).never

      current_connections = @beanstalk.send(:stats_connections)['current-connections']
      client = @beanstalk.direct_connection_client
      assert_equal "WATCHING 2\r\n", client.transmit("watch foo\r\n")
      assert_equal "WATCHING 1\r\n", client.transmit("ignore default\r\n")
      response = client.transmit("list-tubes-watched\r\n")
      assert_equal "OK 9\r\n---\n- foo\r\n", response
      assert_equal current_connections + 1, @beanstalk.send(:stats_connections)['current-connections']
    end


    should 'reset @async_response after receiving message' do
      message = 'foo'
      client = @beanstalk.direct_connection_client
      connection = client.instance_variable_get(:@connection)
      connection.expects(:execute)
      client.instance_variable_set(:@async_response, message)
      assert_equal message, client.transmit("stats\r\n")
      assert_nil client.instance_variable_get(:@async_response)
    end


    should 'wait for and return response' do
      client = @beanstalk.direct_connection_client

      # Overwrite send_data to sleep to ensure delay
      client.instance_eval do
        def self.send_data(*)
          sleep 1
          @async_response = 'foo'
        end
      end

      assert_equal 'foo', client.transmit("stats\r\n")
    end

  end


  context '#close_connection' do

    setup do
      @client = @beanstalk.direct_connection_client
    end


    should 'only execute when connection is alive to avoid stack overflow' do
      @client.instance_variable_get(:@beanstalk).expects(:disconnect).never
      @client.instance_variable_get(:@connection).expects(:alive?).returns(false)
      @client.close_connection
    end


    should 'close connection connection and disconnect from beanstalk' do
      @client.instance_variable_get(:@connection).expects(:close_connection)
      @client.instance_variable_get(:@beanstalk).expects(:disconnect)
      @client.close_connection
    end

  end


  context '#send_data' do

    should 'set @async_response to argument' do
      assert_equal nil, @client.instance_variable_get(:@async_response)
      @client = @beanstalk.direct_connection_client
      message = 'foo'
      @client.send_data(message)
      assert_equal message, @client.instance_variable_get(:@async_response)
    end

  end

end
