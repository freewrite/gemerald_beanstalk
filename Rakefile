require 'bundler/gem_tasks'
require 'rake/testtask'
require 'gemerald_beanstalk'


Rake::TestTask.new do |t|
  t.libs << 'test'
  t.pattern = 'test/**/*_test.rb'
end

task :start_gemerald_beanstalk_test_server do
  Thread.abort_on_exception = true
  server = GemeraldBeanstalk::Server.new(ENV['BIND_ADDRESS'], ENV['PORT'])
  event_reactor = GemeraldBeanstalk::Server.event_reactor_thread
  trap("SIGINT") { event_reactor.kill }
  puts "GemeraldBeanstalk listening on #{server.full_address}"
  event_reactor.join
end

task :default => :test
