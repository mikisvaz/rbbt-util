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

  #def self.severity=(severity)
  #  @severity = severity
  #end

  #def self.severity
  #  @severity
  #end

  SEVERITY_COLOR = ["0;37m", "0;32m", "0;33m", "0;31m","0;37m", "0;32m", "0;33m"].collect{|e| "\033[#{e}"}

  def self.log(message, severity = MEDIUM)
    message ||= ""
    severity_color = SEVERITY_COLOR[severity]
    font_color = {false => "\033[0;37m", true => "\033[0m"}[severity >= INFO]

    if severity >= self.severity and not message.empty?
      str = "\033[0;37m#{Time.now.strftime("[%m/%d/%y-%H:%M:%S]")}#{severity_color}[#{severity.to_s}]\033[0m:#{font_color} " <<  message.strip  << "\033[0m"
      STDERR.puts str
      logfile.puts str unless logfile.nil?
    end
  end

  def self.debug(message)
    log(message, DEBUG)
  end

  def self.low(message)
    log(message, LOW)
  end

  def self.medium(message)
    log(message, MEDIUM)
  end

  def self.high(message)
    log(message, HIGH)
  end

  def self.info(message)
    log(message, INFO)
  end

  def self.warn(message)
    log(message, WARN)
  end

  def self.error(message)
    log(message, ERROR)
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

def ddd(message, file = $stdout)
  Log.debug "DEVEL: " << caller.first
  Log.debug ""
  Log.debug "=> " << message.inspect
  Log.debug ""
end

def ppp(message)
  puts "PRINT: " << caller.first
  puts ""
  puts "=> " << message.inspect
  puts ""
end
