class GemeraldBeanstalk::Connection

  INVALID_REQUEST = GemeraldBeanstalk::Beanstalk::INVALID_REQUEST
  MULTI_PART_REQUEST = GemeraldBeanstalk::Beanstalk::MULTI_PART_REQUEST
  VALID_REQUEST = GemeraldBeanstalk::Beanstalk::VALID_REQUEST

  BEGIN_REQUEST_STATES = [:ready, :multi_part_request_in_progress]

  attr_reader :beanstalk, :mutex, :tube_used, :tubes_watched
  attr_writer :producer, :waiting, :worker


  def alive?
    return @inbound_state != :closed && @oubound_state != :closed
  end


  def begin_multi_part_request(multi_part_request)
    return false unless outbound_ready?
    @multi_part_request = multi_part_request
    @outbound_state = :multi_part_request_in_progress
    return true
  end


  def begin_request
    return false unless BEGIN_REQUEST_STATES.include?(@outbound_state)
    @outbound_state = :request_in_progress
    return true
  end


  def close_connection
    @inbound_state = @outbound_state = :closed
    @connection.close_connection unless @connection.nil?
  end


  def complete_request
    return false unless request_in_progress?
    @outbound_state = :ready
    return true
  end


  def execute(raw_command)
    puts "#{Time.now.to_f}: #{raw_command}" if ENV['VERBOSE']
    parsed_command = response = nil
    @mutex.synchronize do
      return if waiting? || request_in_progress?
      if multi_part_request_in_progress?
        parsed_command = @multi_part_request.push(raw_command)
      else
        parsed_command = beanstalk.parse_command(raw_command)
        case parsed_command.shift
        when INVALID_REQUEST
          response = parsed_command.shift
        when MULTI_PART_REQUEST
          return begin_multi_part_request(parsed_command)
        end
      end
      begin_request
    end
    puts "#{Time.now.to_f}: #{parsed_command.inspect}" if ENV['VERBOSE']
    # Execute command unless parsing already yielded a response
    response ||= beanstalk.execute(self, *parsed_command)
    transmit(response) unless response.nil?
  end


  def ignore(tube, force = false)
    return nil unless @tubes_watched.length > 1 || force
    @tubes_watched.delete(tube)
    return @tubes_watched.length
  end


  def inbound_ready?
    return @inbound_state == :ready
  end


  def initialize(beanstalk, connection = nil)
    @beanstalk = beanstalk
    @connection = connection
    @inbound_state = :ready
    @mutex = Mutex.new
    @outbound_state = :ready
    @tube_used = 'default'
    @tubes_watched = Set.new(%w[default])
  end


  def multi_part_request_in_progress?
    return @outbound_state == :multi_part_request_in_progress
  end


  def outbound_ready?
    return @outbound_state == :ready
  end


  def producer?
    return !!@producer
  end


  def request_in_progress?
    return @outbound_state == :request_in_progress
  end


  def response_received
    return false unless waiting? || timed_out?
    @inbound_state = :ready
    return true
  end


  def timed_out?
    return @inbound_state == :timed_out
  end


  def transmit(message)
    return if !alive? || @connection.nil?
    puts "#{Time.now.to_f}: #{message}" if ENV['VERBOSE']
    @connection.send_data(message)
    complete_request
    response_received
    return self
  end


  def use(tube_name)
    @tube_used = tube_name
  end


  def wait(timeout = nil)
    return false unless inbound_ready?
    @wait_timeout = timeout
    @inbound_state = :waiting
    return true
  end


  def wait_timed_out
    return false unless @inbound_state == :waiting
    @wait_timeout = nil
    @inbound_state = :timed_out
    return true
  end


  def waiting?
    return false unless @inbound_state == :waiting
    return true if @wait_timeout.nil? || @wait_timeout > Time.now.to_f
    wait_timed_out
    return false
  end


  def watch(tube)
    @tubes_watched << tube
    return @tubes_watched.length
  end


  def worker?
    return !!@worker
  end

end
