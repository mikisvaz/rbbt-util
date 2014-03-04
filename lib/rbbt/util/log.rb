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
    attr_accessor :logfile, :severity, :nocolor
  end
  self.nocolor = ENV["RBBT_NOCOLOR"] == 'true'


  def self.logfile
    @logfile = nil
  end

  WHITE, DARK, GREEN, YELLOW, RED = Color::SOLARIZED.values_at :base0, :base00, :green, :yellow, :magenta

  SEVERITY_COLOR = [reset, cyan, green, magenta, blue, yellow, red] #.collect{|e| "\033[#{e}"}
  HIGHLIGHT = "\033[1m"

  def self.color(severity, str = nil)
    return str || "" if nocolor 
    color = SEVERITY_COLOR[severity] if Fixnum === severity
    color = Term::ANSIColor.send(severity) if Symbol === severity and Term::ANSIColor.respond_to? severity 
    if str.nil?
      color
    else
      color + str + color(0)
    end
  end

  def self.return_line
    "\033[1A"
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

  def self.log(message = nil, severity = MEDIUM, &block)
    return if severity < self.severity
    message ||= block.call if block_given?
    return if message.nil?

    time = Time.now.strftime("%m/%d/%y-%H:%M:%S")

    sev_str = severity.to_s

    prefix = time << "[" << color(severity) << sev_str << color(0) << "]"
    message = "" << highlight << message << color(0) if severity >= INFO
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

  def self.exception(e)
    error(e.message)
    error("BACKTRACE:\n" + e.backtrace * "\n") 
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
  puts "=> " << message
  puts ""
end

def fff(object)
  stack = caller
  Log.debug{"#{Log.color :cyan, "FINGERPRINT:"} " << stack.first}
  Log.debug{""}
  Log.debug{require 'rbbt/util/misc'; "=> " << Misc.fingerprint(object) }
  Log.debug{""}
end

def ddd(message, file = $stdout)
  stack = caller
  Log.debug{"#{Log.color :cyan, "DEBUG:"} " << stack.first}
  Log.debug{""}
  Log.debug{"=> " << message.inspect}
  Log.debug{""}
end

def lll(message, file = $stdout)
  stack = caller
  Log.low{"#{Log.color :cyan, "LOW:"} " << stack.first}
  Log.low{""}
  Log.low{"=> " << message.inspect}
  Log.low{""}
end

def mmm(message, file = $stdout)
  stack = caller
  Log.low{"#{Log.color :cyan, "MEDIUM:"} " << stack.first}
  Log.low{""}
  Log.low{"=> " << message.inspect}
  Log.low{""}
end

def hhh(message, file = $stdout)
  stack = caller
  Log.high{"#{Log.color :cyan, "MEDIUM:"} " << stack.first}
  Log.high{""}
  Log.high{"=> " << message.inspect}
  Log.high{""}
end

def iii(message, file = $stdout)
  stack = caller
  Log.info{"#{Log.color :cyan, "MEDIUM:"} " << stack.first}
  Log.info{""}
  Log.info{"=> " << message.inspect}
  Log.info{""}
end

def www(message, file = $stdout)
  stack = caller
  Log.warn{"#{Log.color :cyan, "MEDIUM:"} " << stack.first}
  Log.warn{""}
  Log.warn{"=> " << message.inspect}
  Log.warn{""}
end

if __FILE__ == $0
  Log.severity = 0

  (0..6).each do |level|
    Log.log("Level #{level}", level)
  end
end
