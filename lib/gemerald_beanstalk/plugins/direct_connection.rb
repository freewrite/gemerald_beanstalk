require 'gemerald_beanstalk/plugins/direct_connection/client'

module GemeraldBeanstalk::Plugin::DirectConnection

  def direct_connection_client
    return GemeraldBeanstalk::Plugin::DirectConnection::Client.new(self)
  end

end

GemeraldBeanstalk::Beanstalk.load_plugin(:DirectConnection)
