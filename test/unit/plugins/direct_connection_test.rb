require 'test_helper'

class DirectConnectionTest < GemeraldBeanstalkTest

  context 'autoload' do

    should 'automatically load plugin' do
      GemeraldBeanstalk::Beanstalk.expects(:load_plugin).with(:DirectConnection)
      load 'gemerald_beanstalk/plugins/direct_connection.rb'
    end

  end


  context 'instance methods' do

    setup do
      @beanstalk = GemeraldBeanstalk::Beanstalk.new('localhost:11300')
      @beanstalk.extend(GemeraldBeanstalk::Plugin::DirectConnection)
    end


    context '#direct_connection_client' do

      should 'return a new GemeraldBeanstalk::DirrectConnection' do
        assert_kind_of(
          GemeraldBeanstalk::Plugin::DirectConnection::Client,
          @beanstalk.direct_connection_client
        )
      end

    end

  end

end
