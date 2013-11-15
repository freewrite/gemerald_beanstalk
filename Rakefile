require 'bundler/gem_tasks'
require 'rake/testtask'
require 'gemerald_beanstalk'


Rake::TestTask.new do |t|
  t.libs << 'test'
  t.pattern = 'test/**/*_test.rb'
end

task :gemerald_beanstalk_test do
  require 'coveralls'
  Coveralls.wear!
  server_thread, beanstalk = GemeraldBeanstalk::Server.start(ENV['BIND_ADDRESS'], ENV['PORT'])
  Rake::Task['test'].invoke
  server_thread.kill
end

task :start_gemerald_beanstalk_test_server do
  Thread.abort_on_exception = true
  server_thread, beanstalk = GemeraldBeanstalk::Server.start(ENV['BIND_ADDRESS'], ENV['PORT'])
  trap("SIGINT") { server_thread.kill }
  puts "GemeraldBeanstalk listening on #{beanstalk.address}"
  server_thread.join
end

task :default => :gemerald_beanstalk_test
