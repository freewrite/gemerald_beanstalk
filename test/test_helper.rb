require 'coveralls'
Coveralls.wear!
ENV['TEST'] = 'true'

lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'minitest/autorun'
require 'test/unit'
require 'mocha/setup'
require 'debugger' rescue nil
require 'timeout'
require 'json'
require 'beaneater'
require 'gemerald_beanstalk'

module Interceptor

  def self.beanstalks
    return @@beanstalks ||= Hash.new do |hash, addr|
      hash[addr] = GemeraldBeanstalk::Server.new(addr.split(':').last)
    end
  end


  def self.included(base)
    base.class_eval do
      return if method_defined?(:beaneater_establish_connection)
      alias_method(:beaneater_establish_connection, :establish_connection)
      alias_method(:establish_connection, :gemerald_establish_connection)
    end
  end

  def gemerald_establish_connection
    #@match = address.split(':')
    #@host, @port = @match[0], Integer(@match[1] || Beaneater::Connection::DEFAULT_PORT)
    #Interceptor.beanstalks["#{@host}:#{@port}"]
    beaneater_establish_connection
  end
end

Beaneater::Connection.send(:include, Interceptor)


class MiniTest::Unit::TestCase

  # Cleans up all jobs from tubes
  # cleanup_tubes!(['foo'], @bp)
  def cleanup_tubes!(tubes, bp=nil)
    bp ||= @pool
    tubes.each do |name|
      bp.tubes.find(name).clear
    end
  end


  def assert_kind_of(expected_class, test_subject)
    # Ignore beaneater assertions that Beaneater::Conection#connection should be a TCPSocket
    return super unless caller[0] =~ /test\/beaneater/ && expected_class == TCPSocket && test_subject.kind_of?(GemeraldBeanstalk::Beanstalk)
    return true
  end


  alias_method :old_init, :initialize

  def new_init(*args)
    puts self.class.name
    old_init(*args)
  end

  alias_method :initialize, :new_init
end

