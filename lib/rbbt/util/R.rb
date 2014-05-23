require 'rbbt/util/cmd'
require 'rbbt/tsv'
require 'rbbt/util/R/eval'

module R

  LIB_DIR = File.join(File.expand_path(File.dirname(__FILE__)),'../../../share/Rlib')
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
        Open.write(init_file) do |f|
          f.puts "# Loading basic rbbt environment"
          f.puts "library(utils);\n"
          f.puts "source('#{R::UTIL}');\n"
          f.puts 
          f.puts script
        end
        CMD.cmd("env R_PROFILE='#{init_file}' xterm \"$RHOME/bin/R\"")
    end
  end

  def self.ruby2R(object)
    case object
    when nil
      "NULL"
    when TSV
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

  def self.tsv(file, options = {})
    options = Misc.add_defaults :header_hash => '', :sep => / +/, :type => :list, :key_field => 'ID'
    key_field = Misc.process_options options, :key_field
    clean = CMD.cmd('grep -v WARNING', :in => file, :pipe => true)
    TSV.open(clean, options).tap{|tsv| tsv.key_field = key_field }
  end
end

module TSV

  def R(script, open_options = {})
    TmpFile.with_file do |f|
      Open.write(f, self.to_s)
      io = R.run(
      <<-EOF
## Loading tsv into data
data = rbbt.tsv('#{f}');

#{script.strip}

## Resaving data
if (! is.null(data)){ rbbt.tsv.write('#{f}', data); }
      EOF
      )

      Log.debug(io.read)

      open_options = Misc.add_defaults open_options, :type => :list
      if open_options[:raw]
        Open.read(f)
      else
        tsv = TSV.open(f, open_options) unless open_options[:ignore_output]
        tsv.key_field = open_options[:key] if open_options.include? :key
        tsv.namespace ||= self.namespace if self.namespace
        tsv
      end
    end
  end

  def R_interactive(pre_script = nil)
    TmpFile.with_file do |f|
      Log.debug{"R Script:\n" << pre_script }
      TmpFile.with_file(pre_script) do |script_file|
        Open.write(f, self.to_s)
        script = "data_file = '#{f}';\n"
        script << "script_file = '#{script_file}';\n" if pre_script
        R.interactive(script)
      end
    end
  end
end
