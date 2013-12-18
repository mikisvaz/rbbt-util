require 'term/ansicolor'
require 'rbbt/util/color'

module Log
  extend Term::ANSIColor

  DEBUG    = 0
  LOW      = 1
  MEDIUM   = 2
  HIGH     = 3
  INFO     = 4
  WARN     = 5
  ERROR    = 6

  class << self
    attr_accessor :logfile, :severity
  end


  def self.logfile
    @logfile = nil
  end

  WHITE, DARK, GREEN, YELLOW, RED = Color::SOLARIZED.values_at :base0, :base00, :green, :yellow, :magenta

  SEVERITY_COLOR = [reset, cyan, green, magenta, blue, yellow, red] #.collect{|e| "\033[#{e}"}

  HIGHLIGHT = "\033[1m"

  def self.color(severity)
    SEVERITY_COLOR[severity]
  end

  def self.log(message = nil, severity = MEDIUM, &block)
    return if severity < self.severity
    message ||= block.call if block_given?
    return if message.nil?

    time = Time.now.strftime("%m/%d/%y-%H:%M:%S")

    sev_str = severity.to_s

    prefix = time << "[" << SEVERITY_COLOR[severity] << sev_str << SEVERITY_COLOR[0] << "]"
    message = "" << HIGHLIGHT << message << SEVERITY_COLOR[0] if severity >= INFO
    str = prefix << " " << message

    STDERR.puts str
    logfile.puts str unless logfile.nil?
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
  puts "#{Log::SEVERITY_COLOR[1]}PRINT:#{Log::SEVERITY_COLOR[0]} " << stack.first
  puts ""
  puts "=> " << message
  puts ""
end

def ddd(message, file = $stdout)
  stack = caller
  Log.debug{"#{Log::SEVERITY_COLOR[1]}DEVEL:#{Log::SEVERITY_COLOR[0]} " << stack.first}
  Log.debug{""}
  Log.debug{"=> " << message.inspect}
  Log.debug{""}
end

def fff(object)
  stack = caller
  Log.debug{"#{Log::SEVERITY_COLOR[1]}FINGERPRINT:#{Log::SEVERITY_COLOR[0]} " << stack.first}
  Log.debug{""}
  Log.debug{require 'rbbt/util/misc'; "=> " << Misc.fingerprint(object) }
  Log.debug{""}
end


if __FILE__ == $0
  Log.severity = 0

  (0..6).each do |level|
    Log.log("Level #{level}", level)
  end
end
