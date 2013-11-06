require 'coveralls'
Coveralls.wear!
ENV['TEST'] = 'true'

lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'test/unit'
require 'mocha/setup'
require 'minitest/autorun'
require 'minitest/should'
require 'debugger' rescue nil

require 'securerandom'

require 'beaneater'
require 'gemerald_beanstalk'



class BeanstalkIntegrationTest < MiniTest::Should::TestCase

  class << self

    def address(custom_address = '0.0.0.0')
      return @address ||= custom_address
    end

    def tubes
      return @tubes ||= []
    end

  end

  teardown do
    unless @client.nil? || @client.connection.nil?
      cleanup_tubes
      client.close
    end
  end

  def address
    return self.class.address
  end

  def build_client
    Beaneater::Connection.new(address)
  end

  def cleanup_tubes
    pool = Beaneater::Pool.new([address])
    self.class.tubes.each do |tube_name|
      pool.tubes.find(tube_name).clear
    end
    pool.connections.each(&:close)
    self.class.tubes.clear
  end

  def client
    @client ||= build_client
  end

  def initialize(*)
    @tubes = []
    super
  end

  def generate_tube_name
    tube = uuid
    self.class.tubes << tube
    return tube
  end

  def tube_name
    return @tube_name ||= generate_tube_name
  end

  def uuid
    SecureRandom.uuid
  end

end
