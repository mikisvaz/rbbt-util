require 'rbbt/util/cmd'

module R

  LIB_DIR = File.join(File.expand_path(File.dirname(__FILE__)),'../../../share/lib/R')
  UTIL    = File.join(LIB_DIR, 'util.R')

  def self.run(command, options = {})
    cmd = "source('#{UTIL}');\n"
    case
    when IO === command
      cmd << command.read
    when File.exists?(command)
      cmd << File.open(command, 'r') do |f| f.read end
    else
      cmd << command
    end

    Log.debug "R Script:\n#{ cmd }"

    CMD.cmd('R --vanilla --slave', options.merge(:in => cmd))
  end

end
