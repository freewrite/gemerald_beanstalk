# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'gemerald_beanstalk/version'

Gem::Specification.new do |spec|
  spec.name          = 'gemerald_beanstalk'
  spec.version       = GemeraldBeanstalk::VERSION
  spec.authors       = ['Freewrite.org']
  spec.email         = ['dev@freewrite.org']
  spec.description   = %q{RubyGem implementation of beanstalkd}
  spec.summary       = %q{Gemerald Beanstalk offers a Ruby implementation of beanstalkd for testing and other uses.}
  spec.homepage      = ''
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^test/})
  spec.require_paths = ['lib']

  spec.add_dependency 'eventmachine'
  spec.add_development_dependency 'bundler', '~> 1.3'
  spec.add_development_dependency 'rake'
end
