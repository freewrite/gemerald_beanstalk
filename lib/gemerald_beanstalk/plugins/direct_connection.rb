require 'forwardable'

module GemeraldBeanstalk::Plugin::DirectConnection

  def direct_connection_client
    return GemeraldBeanstalk::DirectConnection.new(self)
  end

end

class GemeraldBeanstalk::DirectConnection

  extend Forwardable

  def_delegators :@connection, :transmit

  def initialize(beanstalk)
    @connection = GemeraldBeanstalk::Connection.new(beanstalk, self)
    @async_fiber = Fiber.new do
      loop do
        Fiber.yield(@async_response)
      end
    end
    @async_response = nil
  end


  def transmit(message)
    immediate_response = @connection.execute(message)
    return immediate_response unless immediate_response.nil?
    while (async_response = @async_fiber.resume).nil?
      sleep 0.1
    end
    @async_response = nil
    return async_response
  end

  private

  def close_connection
    # noop
  end

  def send_data(message)
    @async_response = message
  end

end

GemeraldBeanstalk::Beanstalk.load_plugin(:DirectConnection)
