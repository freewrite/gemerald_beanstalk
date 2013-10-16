require 'set'

class GemeraldBeanstalk::Connection

  COMMAND_PARSER_REGEX = /(?<command>.*?)(?:\r\n(?<body>.*))?\r\n\z/m
  attr_reader :beanstalk, :tube_used, :tubes_watched
  attr_writer :producer, :waiting, :worker

  def alive?
    return !closed?
  end

  def close_connection
    (@connection.close_connection rescue nil) unless @connection.nil?
    @closed = true
  end


  def closed?
    return @closed || false
  end


  def deadline_pending?
    return false if @deadline.nil?
    return true if Time.now.to_f <= @deadline

    @deadline = nil
    return false
  end


  def execute(raw_command)
    if @multi_part_header.nil?
      parsed_command = parse_command(raw_command)
      return if parsed_command.nil?
      if parsed_command[0] == 'put' && parsed_command.last.nil?
        @multi_part_header = parsed_command
        return
      end
    else
      @multi_part_header[-1] = raw_command
      parsed_command = @multi_part_header
      @multi_part_header = nil
    end
    #puts parsed_command.inspect
    response = beanstalk.execute(self, *parsed_command)
    #puts response.inspect
    return response
  end


  def ignore(tube)
    @tubes_watched.delete(tube)
  end


  def initialize(beanstalk, connection = nil)
    @beanstalk = beanstalk
    @connection = connection
    @tube_used = 'default'
    @tubes_watched = Set.new(%w[default])
  end


  def parse_command(raw_command)
    command_lines = raw_command.match(COMMAND_PARSER_REGEX)
    return nil if command_lines.nil?
    command_params = command_lines[:command].split(/\s/)
    command_params.push(nil) if command_lines[:command][-1] =~ /\s/
    command_params.push("#{command_lines[:body]}\r\n") if command_params[0] == 'put' && !command_lines[:body].nil?
    return command_params
  end


  def producer?
    return !!@producer
  end


  def set_deadline(deadline)
    @deadline = deadline
  end


  def use(tube)
    @tube_used = tube
  end


  def watch(tube)
    @tubes_watched << tube
  end


  def waiting?
    return !!@waiting
  end


  def worker?
    return !!@worker
  end

end
