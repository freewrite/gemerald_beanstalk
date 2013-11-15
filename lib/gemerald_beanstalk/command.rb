class GemeraldBeanstalk::Command

  COMMAND_PARSER_REGEX = /(?<command>.*?)(?:\r\n(?<body>.*))?\r\n\z/m
  TRAILING_SPACE_REGEX = /\s+\z/
  WHITE_SPACE_REGEX = / /
  ZERO_STRING_REGEX = /^0+[^1-9]*/

  VALID_TUBE_NAME_REGEX = /\A[a-zA-Z0-9_+\/;.$()]{1}[a-zA-Z0-9_\-+\/;.$()]*\z/

  BAD_FORMAT = GemeraldBeanstalk::Beanstalk::BAD_FORMAT
  UNKNOWN_COMMAND = GemeraldBeanstalk::Beanstalk::UNKNOWN_COMMAND

  COMMANDS = [
    :bury, :delete, :ignore, :kick, :'kick-job', :'list-tubes', :'list-tube-used', :'list-tubes-watched',
    :'pause-tube', :peek, :'peek-buried', :'peek-delayed', :'peek-ready', :put, :quit, :release, :reserve,
    :'reserve-with-timeout', :stats, :'stats-job', :'stats-tube', :touch, :use, :watch,
  ]

  COMMAND_METHOD_NAMES = Hash[COMMANDS.zip(COMMANDS)].merge!({
    :'kick-job' => 'kick_job', :'list-tubes' => 'list_tubes', :'list-tube-used' => 'list_tube_used',
    :'list-tubes-watched' => :'list_tubes_watched', :'pause-tube' => 'pause_tube', :'peek-buried' => 'peek_buried',
    :'peek-delayed' => 'peek_delayed', :'peek-ready' => 'peek_ready', :'reserve-with-timeout' => 'reserve_with_timeout',
    :'stats-job' => 'stats_job', :'stats-tube' => 'stats_tube',
  })

  COMMANDS_RECOGNIZED_WITHOUT_SPACE_AFTER_COMMAND = [
    :'list-tubes', :'list-tube-used', :'list-tubes-watched', :'peek-buried', :'peek-delayed', :'peek-ready',
    :'pause-tube', :quit, :reserve, :'reserve-with-timeout', :stats, :'stats-job', :'stats-tube',
  ]

  COMMANDS_REQUIRING_SPACE_AFTER_COMMAND = COMMANDS - COMMANDS_RECOGNIZED_WITHOUT_SPACE_AFTER_COMMAND

  attr_reader :command, :connection, :error
  attr_writer :body


  def arguments
    if command == :put
      return @args[0, @argument_cardnality].unshift(connection).push(body)
    else
      return @args[0, @argument_cardnality].unshift(connection)
    end
  end


  def body
    return "#{@body}\r\n"
  end


  def initialize(raw_command, connection)
    @connection = connection
    parse(raw_command)
  end


  def method_name
    return COMMAND_METHOD_NAMES[command]
  end


  def multi_part_request?
    return command == :put && body.nil?
  end


  def to_s
    return "#{command} #{@args.join(' ')}"
  end


  def valid?
    return @valid
  end

  private

  def bad_format!
    invalidate(BAD_FORMAT)
  end


  def bury
    @argument_cardnality = 2
    return requires_no_space_after_line &&
    requires_exact_argument_count &&
    requires_valid_integer(@args[0]) &&
    requires_valid_integer(@args[1], :positive => true)
  end


  def delete
    @argument_cardnality = 1
    return true
  end


  def ignore
    @argument_cardnality = 1
    return requires_no_space_after_line &&
    requires_exact_argument_count &&
    requires_valid_tube_name(@args[0])
  end


  def invalidate(error)
    @error = error
    @value = false
  end


  def kick
    @argument_cardnality = 1
    return requires(@args[0]) &&
    requires_valid_integer(@args[0], :allow_trailing => true)
  end


  def kick_job
    @argument_cardnality = 1
    return true
  end


  def list_tubes
    @argument_cardnality = 0
    return requires_only_command
  end


  def list_tube_used
    @argument_cardnality = 0
    return requires_only_command
  end


  def list_tubes_watched
    @argument_cardnality = 0
    return requires_only_command
  end


  def parse(raw_command)
    @command_lines = raw_command.match(COMMAND_PARSER_REGEX)
    if @command_lines.nil?
      return unknown_command!
    end

    @args = @command_lines[:command].split(WHITE_SPACE_REGEX)
    @command = @args.shift.to_sym rescue nil

    @space_after_command = @command && !!(raw_command[@command.length] =~ WHITE_SPACE_REGEX)
    @body = @command_lines[:body]

    return unknown_command! unless valid_command?

    return bad_format! unless send(method_name)
    @valid = true
  end


  def pause_tube
    @argument_cardnality = 2
    return requires_no_space_after_line &&
    requires(@args[0]) &&
    requires_valid_integer(@args[1]) &&
    requires_valid_tube_name(@args[0])
  end


  def peek
    @argument_cardnality = 1
    return true
  end


  def peek_buried
    @argument_cardnality = 0
    return requires_only_command
  end


  def peek_delayed
    @argument_cardnality = 0
    return requires_only_command
  end


  def peek_ready
    @argument_cardnality = 0
    return requires_only_command
  end


  def put
    @argument_cardnality = 5
    return false unless @args.all? do |arg|
      requires_valid_integer(arg, :positive => true)
    end

    # Handle weird case where BAD_FORMAT, but increments stats
    is_valid = requires_no_space_after_line
    unless is_valid
      connection.beanstalk.adjust_stats_cmd_put
    end
    return is_valid
  end


  def quit
    @argument_cardnality = 0
    return requires_only_command
  end


  def release
    @argument_cardnality = 3
    return requires_no_space_after_line &&
    requires_exact_argument_count &&
    requires_valid_integer(@args[0]) &&
    requires_valid_integer(@args[1], :positive => true) &&
    requires_valid_integer(@args[2], :positive => true)
  end


  def requires(arg)
    return true unless arg.nil?

    bad_format!
    return false
  end


  def requires_exact_argument_count
    return true if @args.length == @argument_cardnality

    bad_format!
    return false
  end


  def requires_no_space_after_command
    return true unless @space_after_command

    bad_format!
    return false
  end


  def requires_no_space_after_line
    return true unless @command_lines[:command] =~ TRAILING_SPACE_REGEX

    bad_format!
    return false
  end


  def requires_only_command
    return requires_no_space_after_command && requires_exact_argument_count
  end


  def requires_space_after_command
    return true if @space_after_command

    bad_format!
    return false
  end


  def requires_valid_integer(arg, opts = {})
    arg_to_i = arg.to_i
    if opts[:allow_trailing]
      if arg_to_i == 0 && arg !~ ZERO_STRING_REGEX
        bad_format!
        return false
      end
    else
      if arg_to_i.to_s != arg
        bad_format!
        return false
      end
    end

    return true if opts[:positive] ? arg_to_i >= 0 : true

    bad_format!
    return false
  end


  def requires_valid_tube_name(tube_name)
    return true if valid_tube_name?(tube_name)

    bad_format!
    return false
  end


  def reserve
    @argument_cardnality = 0
    return requires_only_command
  end


  def reserve_with_timeout
    @argument_cardnality = 1
    return requires_space_after_command
  end


  def stats
    @argument_cardnality = 0
    return requires_only_command
  end


  def stats_job
    @argument_cardnality = 1
    return requires_space_after_command
  end


  def stats_tube
    @argument_cardnality = 1
    return requires_space_after_command &&
    requires_no_space_after_line &&
    requires_valid_tube_name(@args[0])
  end


  def touch
    @argument_cardnality = 1
    return true
  end


  def use
    @argument_cardnality = 1
    return requires_no_space_after_line &&
    requires_exact_argument_count &&
    requires_valid_tube_name(@args[0])
  end


  def unknown_command!
    invalidate(UNKNOWN_COMMAND)
  end


  def valid_command?
    return COMMANDS_RECOGNIZED_WITHOUT_SPACE_AFTER_COMMAND.include?(command) || (
      COMMANDS_REQUIRING_SPACE_AFTER_COMMAND.include?(command) && @space_after_command
    )
  end


  def valid_tube_name?(tube_name)
    return !tube_name.nil? && tube_name.bytesize <= 200 && VALID_TUBE_NAME_REGEX =~ tube_name
  end


  def watch
    @argument_cardnality = 1
    return requires_no_space_after_line &&
    requires_exact_argument_count &&
    requires_valid_tube_name(@args[0])
  end

end
