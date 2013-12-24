require 'coveralls'
Coveralls.wear!

require 'test/unit'
require 'mocha/setup'
require 'minitest/autorun'
require 'minitest/should'

lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'gemerald_beanstalk'
$server ||= GemeraldBeanstalk::Server.new(ENV['BIND_ADDRESS'], ENV['PORT'])

class GemeraldBeanstalkTest < MiniTest::Should::TestCase
end
