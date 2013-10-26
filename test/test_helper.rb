require 'coveralls'
Coveralls.wear!
ENV['TEST'] = 'true'

lib = File.expand_path('../../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

require 'test/unit'
require 'mocha/setup'
require 'debugger' rescue nil
require 'gemerald_beanstalk'
