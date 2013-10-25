class GemeraldBeanstalk::Connection

  COMMAND_PARSER_REGEX = /(?<command>.*?)(?:\r\n(?<body>.*))?\r\n\z/m

  attr_reader :beanstalk, :tube_used, :tubes_watched
  attr_writer :producer, :waiting, :worker

  state_machine :inbound_state, :initial => :ready, :namespace => :inbound do
    event :response_received do
      transition :waiting => :ready
    end


    event :wait do
      transition :ready => :waiting
    end


    event :wait_timed_out do
      @wait_timeout = nil
      transition :waiting => :timed_out
    end


    event :close do
      transition any => :closed
    end

  end

  state_machine :outbound_state, :initial => :ready, :namespace => :outbound do
    event :begin_request do
      transition [:ready, :multi_part_request_pending] => :request_in_progress
    end

    event :multi_part_request_start do
      transition :ready => :multi_part_request_pending
    end


    event :complete_request do
      transition :request_in_progress => :ready
    end


    event :close do
      transition any => :closed
    end

  end


  def alive?
    return !(inbound_closed? || outbound_closed?)
  end


  def close_connection
      close_inbound
      close_outbound
      @connection.close_connection unless @connection.nil?
  end


  def execute(raw_command)
    @mutex.synchronize do
      return if inbound_state_name == :waiting || outbound_state_name == :request_in_progress
      if outbound_multi_part_request_pending?
        parsed_command = @multi_part_request.push(raw_command)
      else
        parsed_command = parse_command(raw_command)
        return if parsed_command.nil? || outbound_multi_part_request_pending?
      end
      begin_request_outbound
      #puts "#{Time.now.to_f}: #{parsed_command.inspect}"
      response = beanstalk.execute(self, *parsed_command)
      transmit(response) unless response.nil?
    end
  end


  def ignore(tube)
    return nil if @tubes_watched.length == 1
    @tubes_watched.delete(tube)
    return @tubes_watched.length
  end


  def initialize(beanstalk, connection = nil)
    @beanstalk = beanstalk
    @connection = connection
    @mutex = Mutex.new
    @tube_used = 'default'
    @tubes_watched = Set.new(%w[default])

    # Initialize state machine
    super()
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
        multi_part_request_start_outbound
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


  def transmit(message)
    return if !alive? || @connection.nil?
    #puts "#{Time.now.to_f}: #{message}"
    @connection.send_data(message)
    complete_request_outbound
    response_received_inbound if waiting?
  end


  def use(tube_name)
    @tube_used = tube_name
  end


  def wait_inbound(timeout = nil, *args)
    return false unless super
    @wait_timeout = timeout
  end


  def waiting?
    return false unless self.inbound_waiting?
    return true if @wait_timeout.nil? || @wait_timeout > Time.now.to_f
    wait_timed_out_inbound
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
