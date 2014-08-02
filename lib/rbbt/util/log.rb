require 'term/ansicolor'
require 'rbbt/util/color'
require 'rbbt/util/log/progress'

class MockMutex
  def synchronize
    yield
  end
end

module Log
  extend Term::ANSIColor


  #ToDo: I'm not sure if using a Mutex here really gives troubles in CPU concurrency
  #LOG_MUTEX = MockMutex.new
  LOG_MUTEX = Mutex.new

  SEVERITY_NAMES ||= begin
                     names = %w(DEBUG LOW MEDIUM HIGH INFO WARN ERROR ) 
                     names.each_with_index do |name,i|
                       eval "#{ name } = #{ i }" 
                     end
                     names
                   end

  def self.last_caller(stack)
    line = nil
    while line.nil? or line =~ /util\/log\.rb/ and stack.any?
      line = stack.shift 
    end
    line ||= caller.first
  end

  def self.ignore_stderr
    LOG_MUTEX.synchronize do
      backup_stderr = STDERR.dup
      File.open('/dev/null', 'w') do |f|
        STDERR.reopen(f)
        begin
          yield
        ensure
          STDERR.reopen backup_stderr
          backup_stderr.close
        end
      end
    end
  end

  def self.get_level(level)
    case level
    when Fixnum
      level
    when String
      begin
        Log.const_get(level.upcase)
      rescue
        Log.exception $!
      end
    when Symbol
      get_level(level.to_s)
    end || 0
  end

  class << self
    attr_accessor :logfile, :severity, :nocolor, :tty_size
  end

  self.nocolor = ENV["RBBT_NOCOLOR"] == 'true'

  self.ignore_stderr do
    require 'nokogiri'
    self.tty_size = begin
                      require "highline/system_extensions.rb"
                      HighLine::SystemExtensions.terminal_size.first 
                    rescue Exception
                      nil
                    end
  end

  def self.with_severity(level)
    orig = Log.severity
    begin
      Log.severity = level
      yield
    ensure
      Log.severity = orig
    end
  end

  def self.logfile
    @logfile = nil
  end

  WHITE, DARK, GREEN, YELLOW, RED = Color::SOLARIZED.values_at :base0, :base00, :green, :yellow, :magenta

  SEVERITY_COLOR = [reset, cyan, green, magenta, blue, yellow, red] #.collect{|e| "\033[#{e}"}
  HIGHLIGHT = "\033[1m"

  def self.uncolor(str)
    Term::ANSIColor.uncolor(str)
  end

  def self.reset_color
    reset
  end

  def self.color(severity, str = nil, reset = false)
    return str || "" if nocolor 
    color = reset ? Term::ANSIColor.reset : ""
    color << SEVERITY_COLOR[severity] if Fixnum === severity
    color << Term::ANSIColor.send(severity) if Symbol === severity and Term::ANSIColor.respond_to? severity 
    if str.nil?
      color
    else
      color + str.to_s + self.color(0)
    end
  end

  def self.return_line
    nocolor ? "" : "\033[1A"
  end

  def self.clear_line(out = STDOUT)
    out.puts Log.return_line << " " * (Log.tty_size || 80) << Log.return_line unless nocolor
  end

  def self.highlight(str = nil)
    if str.nil?
      return "" if nocolor
      HIGHLIGHT
    else
      return str if nocolor
      HIGHLIGHT + str + color(0)
    end
  end

  LAST = "log"
  def self.log(message = nil, severity = MEDIUM, &block)
    return if severity < self.severity 
    message ||= block.call if block_given?
    return if message.nil?

    time = Time.now.strftime("%m/%d/%y-%H:%M:%S")

    sev_str = severity.to_s

    prefix = time << "[" << color(severity) << sev_str << color(0)<<"]"
    message = "" << highlight << message << color(0) if severity >= INFO
    str = prefix << " " << message

    LOG_MUTEX.synchronize do
      STDERR.puts str
      Log::LAST.replace "log"
      logfile.puts str unless logfile.nil?
      nil
    end
  end

  def self.log_obj_inspect(obj, level, file = $stdout)
    stack = caller

    line = Log.last_caller stack

    level = Log.get_level level
    name = Log::SEVERITY_NAMES[level] + ": "
    Log.log Log.color(level, name, true) << line, level
    Log.log "", level
    Log.log Log.color(level, "=> ", true) << obj.inspect, level
    Log.log "", level
  end

  def self.log_obj_fingerprint(obj, level, file = $stdout)
    stack = caller

    line = Log.last_caller stack

    level = Log.get_level level
    name = Log::SEVERITY_NAMES[level] + ": "
    Log.log Log.color(level, name, true) << line, level
    Log.log "", level
    Log.log Log.color(level, "=> ", true) << Misc.fingerprint(obj), level
    Log.log "", level
  end

  def self.debug(message = nil, &block)
    log(message, DEBUG, &block)
  end

  def self.low(message = nil, &block)
    log(message, LOW, &block)
  end

  def self.medium(message = nil, &block)
    log(message, MEDIUM, &block)
  end

  def self.high(message = nil, &block)
    log(message, HIGH, &block)
  end

  def self.info(message = nil, &block)
    log(message, INFO, &block)
  end

  def self.warn(message = nil, &block)
    log(message, WARN, &block)
  end

  def self.error(message = nil, &block)
    log(message, ERROR, &block)
  end

  def self.exception(e)
    stack = caller
    error([e.class.to_s, e.message].compact * ": " )
    error("BACKTRACE: " << Log.last_caller(caller) << "\n" + color_stack(e.backtrace)*"\n")
  end

  def self.color_stack(stack)
    stack.collect do |line|
      line = line.sub('`',"'")
      color = :green if line =~ /workflow/
      color = :blue if line =~ /rbbt-/
      Log.color color, line
    end
  end

  def self.stack(stack)
    LOG_MUTEX.synchronize do

      STDERR.puts Log.color :magenta, "Stack trace: " << Log.last_caller(caller)
      color_stack(stack).each do |line|
        STDERR.puts line
      end
    end
  end

  case ENV['RBBT_LOG'] 
  when 'DEBUG' 
    self.severity = DEBUG
  when 'LOW' 
    self.severity = LOW
  when 'MEDIUM' 
    self.severity = MEDIUM
  when 'HIGH' 
    self.severity = HIGH
  when nil
    self.severity = INFO
  else
    self.severity = ENV['RBBT_LOG'].to_i
  end
end

def ppp(message)
  stack = caller
  puts "#{Log.color :cyan, "PRINT:"} " << stack.first
  puts ""
  puts Log.color(:cyan, "=> ") << message
  puts ""
end

def fff(object)
  stack = caller
  Log.debug{"#{Log.color :cyan, "FINGERPRINT:"} " << stack.first}
  Log.debug{""}
  Log.debug{require 'rbbt/util/misc'; "=> " << Misc.fingerprint(object) }
  Log.debug{""}
end

def ddd(obj, file = $stdout)
  Log.log_obj_inspect(obj, :debug, file)
end

def lll(obj, file = $stdout)
  Log.log_obj_inspect(obj, :low, file)
end

def mmm(obj, file = $stdout)
  Log.log_obj_inspect(obj, :medium, file)
end

def iii(obj, file = $stdout)
  Log.log_obj_inspect(obj, :info, file)
end

def wwww(obj, file = $stdout)
  Log.log_obj_inspect(obj, :warn, file)
end

def eee(obj, file = $stdout)
  Log.log_obj_inspect(obj, :error, file)
end

def ddf(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :debug, file)
end

def llf(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :low, file)
end

def mmf(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :medium, file)
end

def iif(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :info, file)
end

def wwwf(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :warn, file)
end

def eef(obj, file = $stdout)
  Log.log_obj_fingerprint(obj, :error, file)
end

if __FILE__ == $0
  Log.severity = 0

  (0..6).each do |level|
    Log.log("Level #{level}", level)
  end

  require 'rbbt/util/misc'
  eee [1,2,3]
  eef [1,2,3]
end
