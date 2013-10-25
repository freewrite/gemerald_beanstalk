require 'debugger'
class GemeraldBeanstalk::Connection

  COMMAND_PARSER_REGEX = /(?<command>.*?)(?:\r\n(?<body>.*))?\r\n\z/m

  BEGIN_REQUEST_STATES = [:ready, :multi_part_request_in_progress]

  attr_reader :beanstalk, :tube_used, :tubes_watched
  attr_writer :producer, :waiting, :worker


  def alive?
    return @inbound_state != :closed && @oubound_state != :closed
  end


  def begin_multi_part_request
    return false unless outbound_ready?
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
    @mutex.synchronize do
      return if waiting? || request_in_progress?
      if multi_part_request_in_progress?
        parsed_command = @multi_part_request.push(raw_command)
      else
        parsed_command = parse_command(raw_command)
        return if parsed_command.nil? || multi_part_request_in_progress?
      end
      begin_request
      # puts "#{Time.now.to_f}: #{parsed_command.inspect}"
      response = beanstalk.execute(self, *parsed_command)
      transmit(response) unless response.nil?
    end
  end


  def ignore(tube)
    return nil if @tubes_watched.length == 1
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


  def parse_command(raw_command)
    command_lines = raw_command.match(COMMAND_PARSER_REGEX)
    return if command_lines.nil?

    command_params = command_lines[:command].split(/\s/)
    if command_lines[:command][-1] =~ /\s/
      command_params = ['bad_format!']
    elsif command_params[0] == 'bad_format!'
      command_params = []
    elsif command_params[0] == 'put'
      if command_lines[:body].nil?
        begin_multi_part_request
        @multi_part_request = command_params
      else
        command_params.push("#{command_lines[:body]}\r\n")
      end
    end
    return command_params
  end


  def producer?
    return !!@producer
  end


  def request_in_progress?
    return @outbound_state == :request_in_progress
  end


  def response_received
    return false unless waiting?
    @inbound_state = :ready
    return true
  end


  def timed_out?
    return @inbound_state == :timed_out
  end


  def transmit(message)
    return if !alive? || @connection.nil?
    # puts "#{Time.now.to_f}: #{message}"
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
