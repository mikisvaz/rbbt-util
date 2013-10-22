module Log

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

  WHITE, DARK, GREEN, YELLOW, RED = ["0;37m", "0m", "0;32m", "0;33m", "0;31m"].collect{|e| "\033[#{e}"}

  SEVERITY_COLOR = [WHITE, GREEN, YELLOW, RED,WHITE, GREEN, YELLOW].collect{|e| "\033[#{e}"}

  def self.log(message = nil, severity = MEDIUM, &block)
    message ||= block
    return if message.nil?
    severity_color = SEVERITY_COLOR[severity]
    font_color = {true => WHITE, false => DARK}[severity >= INFO]

    return if severity < self.severity
    message = message.call if Proc === message
    return if message.nil? or message.empty?

    str = "\033[0;37m#{Time.now.strftime("[%m/%d/%y-%H:%M:%S]")}#{severity_color}[#{severity.to_s}]\033[0m:#{font_color} " <<  message.strip  << "\033[0m"
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
