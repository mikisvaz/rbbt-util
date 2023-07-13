require 'rbbt/util/cmd'
require 'rbbt/tsv'
require 'rbbt/util/R/eval'
require 'rbbt/util/R/plot'
require 'rbbt/util/R/model'

module R

  LIB_DIR = Path.setup(File.join(File.expand_path(File.dirname(__FILE__)),'../../../share/Rlib'))
  UTIL    = File.join(LIB_DIR, 'util.R')
  PLOT    = File.join(LIB_DIR, 'plot.R')

  def self.run(command, source = nil, options = nil)
    source, options = nil, source if options.nil? and Hash === source
    options = {} if options.nil?
    monitor = options.delete :monitor

    cmd =<<-EOF
# Loading basic rbbt environment
source('#{UTIL}');

    EOF

    require_sources  = source.collect{|source|
      source = R::LIB_DIR["#{source.to_s}.R"] if R::LIB_DIR["#{source.to_s}.R"].exists?
      "source('#{source}')"
    } * ";\n" if Array === source and source.any?

    cmd << require_sources + "\n\n" if require_sources

    case
    when IO === command
      cmd << command.read
    when File.exist?(command)
      cmd << File.open(command, 'r') do |f| f.read end
    else
      cmd << command
    end

    Log.debug{"R Script:\n#{ cmd }"}

    if monitor
      #io = CMD.cmd('R --no-save --quiet', options.merge(:in => cmd, :pipe => true, :log => true))
      io = CMD.cmd('R --no-save --quiet', options.merge(:in => cmd, :pipe => true, :log => true, :xvfb => options[:xvfb]))
      while line = io.gets
        case monitor
        when Proc
          monitor.call line
        else
          Log.debug "R: " <<  line
        end
      end
      io.join if io.respond_to? :join
    else
      CMD.cmd('R --no-save --slave --quiet', options.merge(:in => cmd, :xvfb => options[:xvfb]))
    end
  end

  def self.interactive(script, source = [], options = {})
    TmpFile.with_file(script) do |script_file|
      TmpFile.with_file do |init_file|

        cmd = <<-EOF
  # Loading basic rbbt environment"
library(utils, quietly=TRUE);
library(grDevices,quietly=TRUE)   
source('#{R::UTIL}');
EOF

    require_sources  = source.collect{|source|
      source = R::LIB_DIR["#{source.to_s}.R"] if R::LIB_DIR["#{source.to_s}.R"].exists?
      "source('#{source}')"
    } * ";\n" if Array === source and source.any?

    cmd << require_sources + "\n\n" if require_sources

        cmd += <<-EOF

rbbt.require('readr')
interactive.script.file = '#{script_file}'
interactive.script = read_file(interactive.script.file)

cat(interactive.script)

source(interactive.script.file)
        EOF

        Open.write init_file, cmd
        CMD.cmd("env R_PROFILE='#{init_file}' xterm \"$R_HOME/bin/R\"")
      end
    end
  end

  def self.console(script, options = {})
    TmpFile.with_file do |init_file|
       Open.write(init_file) do |f|
          f.puts "# Loading basic rbbt environment"
          f.puts "library(utils);\n"
          f.puts "source('#{R::UTIL}');\n"
          f.puts 
          f.puts script
        end

       pid = Process.fork do |ppid|
         ENV["R_PROFILE"] = init_file
         exec("R")
       end

       begin
         Process.waitpid pid
       rescue Interrupt
         if Misc.pid_exists? pid
           Process.kill "INT", pid
           retry
         else
           raise $!
         end
       rescue Exception
         Process.kill 9, pid if Misc.pid_exists? pid
         raise $!
       ensure
         Process.waitpid pid if Misc.pid_exists? pid
       end

    end
  end

  def self.ruby2R(object)
    case object
    when Float::INFINITY
      "Inf"
    when nil
      "NULL"
    when ":NA"
      "NA"
    when TSV
      "matrix(#{R.ruby2R object.values},dimnames=list(#{R.ruby2R object.keys}, #{R.ruby2R object.fields}))"
    when Symbol
      "#{ object }"
    when String
      object[0] == ":" ? object[1..-1] : "'#{ object }'"
    when Numeric
      object
    when TrueClass
      "TRUE"
    when FalseClass
      "FALSE"
    when Array
      "c(#{object.collect{|e| ruby2R(e) } * ", "})"
    when Hash
      "list(" << object.collect{|k,v| [k, ruby2R(v)] * "="} * ", " << ")"
    else
      raise "Type of object not known: #{ object.inspect }"
    end
  end

  def self.hash2Rargs(hash)
    hash.collect do |k,v|
      [k.to_s, ruby2R(v)] * "="
    end * ", "
  end

  def self.tsv(file, options = {})
    options = Misc.add_defaults :header_hash => '', :sep => / +/, :type => :list, :key_field => 'ID'
    key_field = Misc.process_options options, :key_field
    clean = CMD.cmd('grep -v WARNING', :in => file, :pipe => true)
    TSV.open(clean, options).tap{|tsv| tsv.key_field = key_field }
  end
end

module TSV

  def R(script, source = nil, open_options = {})
    open_options, source = source, nil if Hash === source

    source ||= Misc.process_options open_options, :source
    source = [source] unless Array === source 

    require_sources  = source.collect{|source|
      source = R::LIB_DIR["#{source.to_s}.R"] if R::LIB_DIR["#{source.to_s}.R"].exists?
      "source('#{source}')"
    } * ";\n" if Array === source and source.any?

    script = require_sources + "\n\n" + script if require_sources

    r_options = IndiferentHash.pull_keys open_options, :R

    r_options[:monitor] = open_options[:monitor] if open_options.include?(:monitor)
    r_options[:method] = open_options[:method] if open_options.include?(:method)
    r_options[:debug] = open_options[:debug] if open_options.include?(:debug)
    r_options[:erase] = open_options.delete(:erase) if open_options.include?(:erase)

    r_options[:debug] = true if r_options[:method] == :debug
    if r_options.delete :debug
      r_options[:monitor] = true
      r_options[:method] = :shell
      erase = r_options.include?(:erase) ? r_options[:erase] : false
    else
      erase = r_options.include?(:erase) ? r_options[:erase] : true
    end

    tsv_R_option_str = r_options.delete :open
    tsv_R_option_str = ", "  + tsv_R_option_str if String === tsv_R_option_str and not tsv_R_option_str.empty?

    raw = open_options.delete :raw
    TmpFile.with_file nil, erase do |f|
      Open.write(f, self.to_s)

      script = <<-EOF
## Loading tsv into data
data = rbbt.tsv('#{f}'#{tsv_R_option_str});

#{script.strip}

## Resaving data
if (! is.null(data)){ rbbt.tsv.write('#{f}', data); }
NULL
      EOF

      case r_options.delete :method
      when :eval
        R.eval_run script
      else 
        R.run script, r_options
      end

      open_options = IndiferentHash.add_defaults open_options, :type => :list
      if raw
        Open.read(f)
      else
        tsv = TSV.open(f, open_options) unless open_options[:ignore_output]
        tsv.key_field = open_options[:key] if open_options.include? :key
        tsv.namespace ||= self.namespace if self.namespace
        tsv
      end
    end
  end

  def R_interactive(script = nil, source = [])
    TmpFile.with_file do |data_file|
      Open.write(data_file, self.to_s)

      Log.debug{"R Interactive:\n" << script } if script

      script =<<-EOF
# Loading data
data_file = '#{data_file}'
data = rbbt.tsv(data_file)

# Script
#{script}
      EOF

      R.interactive(script)
    end
  end

  def R_console(pre_script = nil)
    TmpFile.with_file do |f|
      Log.debug{"R Console:\n" << pre_script } if pre_script
      TmpFile.with_file(pre_script) do |script_file|
        Open.write(f, self.to_s)
        script = "data_file = '#{f}';\n"
        script <<  "\n#\{{{Pre-script:\n\n" << pre_script << "\n#}}}Pre-script\n\n"
        R.console(script)
      end
    end
  end
end
