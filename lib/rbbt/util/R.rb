require 'rbbt/util/cmd'
require 'rbbt/util/tsv'

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

    CMD.cmd('R --vanilla --slave --quiet', options.merge(:in => cmd))
  end

end

class TSV
  def R(script, open_options = {})
    TmpFile.with_file do |f|
      Open.write(f, self.to_s)
      Log.debug(R.run(
      <<-EOF
data = rbbt.tsv('#{f}');
#{script.strip}
rbbt.tsv.write('#{f}', data);
      EOF
      ).read)
      open_options = Misc.add_defaults open_options, :type => :list
      TSV.new(f, open_options)
    end
  end
end
