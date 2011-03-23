module Log

  DEBUG    = 0
  LOW      = 1
  MEDIUM   = 2
  HIGH     = 3
  INFO     = 4
  WARN     = 5
  ERROR    = 6

  def self.severity=(severity)
    @@severity = severity
  end

  def self.severity
    @@severity
  end

  SEVERITY_COLOR = ["0;37m", "0;32m", "0;33m", "0;31m", "1;0m" ].collect{|e| "\033[#{e}"}

  def self.log(message, severity = MEDIUM)
    severity_color = SEVERITY_COLOR[severity]
    font_color = {false => "\033[0;37m", true => "\033[0m"}[severity >= INFO]

    STDERR.puts caller.select{|l| l =~ /rbbt/} * "\n" if @@severity == -1 and not message.empty?
    #STDERR.puts "#{Time.now.strftime("[%m/%d/%y-%H:%M:%S]")}[#{severity.to_s}]: " +  message if severity >= @@severity
    STDERR.puts "\033[0;37m#{Time.now.strftime("[%m/%d/%y-%H:%M:%S]")}#{severity_color}[#{severity.to_s}]\033[0m:#{font_color} " <<  message.strip  << "\033[0m" if severity >= @@severity
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
    @@severity = DEBUG
  when 'LOW' 
    @@severity = LOW
  when 'MEDIUM' 
    @@severity = MEDIUM
  when 'HIGH' 
    @@severity = HIGH
  when nil
    @@severity = INFO
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
