require 'rbbt/util/cmd'
require 'rbbt/tsv'

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

  def self.interactive(init_file, options = {})
    CMD.cmd("env R_PROFILE='#{init_file}' xterm R")
  end

  def self.interactive(script, options = {})
    TmpFile.with_file do |init_file|
        Open.write(init_file) do |file|
          profile = File.join(ENV["HOME"], ".Rprofile")
          file.puts "source('#{profile}');\n" if File.exists? profile
          file.puts "source('#{R::UTIL}');\n"
          file.puts script
        end
        CMD.cmd("env R_PROFILE='#{init_file}' xterm R")
    end
  end

end

module TSV

  def R(script, open_options = {})
    TmpFile.with_file do |f|
      Open.write(f, self.to_s)
      Log.debug(R.run(
      <<-EOF
data = rbbt.tsv('#{f}');
#{script.strip}
if (! is.null(data)){ rbbt.tsv.write('#{f}', data); }
      EOF
      ).read)
      open_options = Misc.add_defaults open_options, :type => :list
      TSV.open(f, open_options) unless open_options[:ignore_output]
    end
  end

  def R_interactive(open_options = {})
    TmpFile.with_file do |f|
      Open.write(f, self.to_s)
      R.interactive("data_file = '#{f}';\n")
    end
  end
end

