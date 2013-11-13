require 'bundler/gem_tasks'
require 'rake/testtask'

Rake::TestTask.new do |t|
  t.libs << 'test'
  t.pattern = 'test/**/*_test.rb'
end

task :start_gemerald_beanstalk_test_server do
  require 'gemerald_beanstalk'

  Thread.abort_on_exception = true
  server_thread, beanstalk = GemeraldBeanstalk::Server.start(ENV['BIND_ADDRESS'], ENV['PORT'])
  trap("SIGINT") { server_thread.kill }
  puts "GemeraldBeanstalk listening on #{beanstalk.address}"
  server_thread.join
end

task :default => :test
