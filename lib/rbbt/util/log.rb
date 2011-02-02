module Log

  DEBUG  = 0
  LOW    = 1
  MEDIUM = 2
  HIGH   = 3

  def self.severity=(severity)
    @@severity = severity
  end

  def self.severity
    @@severity
  end

  def self.log(message, severity = MEDIUM)
    STDERR.puts caller * "\n" if @@severity == -1
    STDERR.puts "#{Time.now}[#{severity.to_s}]: " +  message if severity >= @@severity
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

  case ENV['RBBT_LOG']
  when 'DEBUG' 
    @@severity = DEBUG
  when 'LOW' 
    @@severity = LOW
  when 'MEDIUM' 
    @@severity = MEDIUM
  when 'HIGH' 
    @@severity = HIGH
  when nil
    @@severity = HIGH
  else
    @@severity = ENV['RBBT_LOG'].to_i
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
