require 'test_helper'

class BeanstalkTest < GemeraldBeanstalkTest

  context '::load_plugin' do

    should 'raise NameError if unknown plugin' do
      assert_raises(NameError) do
        GemeraldBeanstalk::Beanstalk.load_plugin(:Foo)
      end
    end


    should 'include the given plugin' do
      GemeraldBeanstalk::Beanstalk.load_plugin(:Dummy)
      assert GemeraldBeanstalk::Beanstalk.included_modules.include?(GemeraldBeanstalk::Plugin::Dummy)
    end

  end

end

module GemeraldBeanstalk::Plugin::Dummy; end
