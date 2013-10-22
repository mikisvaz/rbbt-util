require 'rbbt/util/cmd'
require 'rbbt/tsv'

module R

  LIB_DIR = File.join(File.expand_path(File.dirname(__FILE__)),'../../../share/lib/R')
  UTIL    = File.join(LIB_DIR, 'util.R')

  def self.run(command, options = {})
    cmd =<<-EOF
# Loading basic rbbt environment
source('#{UTIL}');

    EOF

    case
    when IO === command
      cmd << command.read
    when File.exists?(command)
      cmd << File.open(command, 'r') do |f| f.read end
    else
      cmd << command
    end

    Log.debug{"R Script:\n#{ cmd }"}

    if options.delete :monitor
      io = CMD.cmd('R --vanilla --slave --quiet', options.merge(:in => cmd, :pipe => true))
      while line = io.gets
        puts line
      end
      nil
    else
      CMD.cmd('R --vanilla --slave --quiet', options.merge(:in => cmd))
    end
  end

  def self.interactive(script, options = {})
    TmpFile.with_file do |init_file|
        Open.write(init_file) do |file|
          file.puts "# Loading basic rbbt environment"
          file.puts "library(utils);\n"
          file.puts "source('#{R::UTIL}');\n"
          file.puts 
          file.puts script
        end
        CMD.cmd("env R_PROFILE='#{init_file}' xterm \"$RHOME/bin/R\"")
    end
  end

  def self.ruby2R(object)
    case object
    when nil
      "NULL"
    when TSV
      #"as.matrix(data.frame(c(#{object.transpose("Field").collect{|k,v| "#{k}=" << R.ruby2R(v)}.flatten * ", "}), row.names=#{R.ruby2R object.keys}))"
      "matrix(#{R.ruby2R object.values},dimnames=list(#{R.ruby2R object.keys}, #{R.ruby2R object.fields}))"
    when Symbol
      "#{ object }"
    when String
      "'#{ object }'"
    when Fixnum, Float
      object
    when Array
      "c(#{object.collect{|e| ruby2R(e) } * ", "})"
    else
      raise "Type of object not known: #{ object.inspect }"
    end
  end

end

module TSV

  def R(script, open_options = {})
    TmpFile.with_file do |f|
      Open.write(f, self.to_s)
      Log.debug(R.run(
      <<-EOF
## Loading tsv into data
data = rbbt.tsv('#{f}');

#{script.strip}

## Resaving data
if (! is.null(data)){ rbbt.tsv.write('#{f}', data); }
      EOF
      ).read)
      open_options = Misc.add_defaults open_options, :type => :list
      if open_options[:raw]
        Open.read(f)
      else
        TSV.open(f, open_options) unless open_options[:ignore_output]
      end
    end
  end

  def R_interactive(pre_script = nil)
    TmpFile.with_file do |f|
      TmpFile.with_file(pre_script) do |script_file|
        Open.write(f, self.to_s)
        script = "data_file = '#{f}';\n"
        script << "script_file = '#{script_file}';\n" if pre_script
        R.interactive(script)
      end
    end
  end
end
