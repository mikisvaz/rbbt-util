module Log
  def self.log(message, severity = nil)
    STDERR.puts message
  end
end
