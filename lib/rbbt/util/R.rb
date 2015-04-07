require 'rbbt/util/cmd'
require 'rbbt/tsv'
require 'rbbt/util/R/eval'

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
      source = R::LIB_DIR["plot.R"] if source == :plot
      "source('#{source}')"
    } * ";\n" if Array === source and source.any?

    cmd << require_sources + "\n\n" if require_sources

    case
    when IO === command
      cmd << command.read
    when File.exists?(command)
      cmd << File.open(command, 'r') do |f| f.read end
    else
      cmd << command
    end

    Log.debug{"R Script:\n#{ cmd }"}

    if monitor
      io = CMD.cmd('R --vanilla --quiet', options.merge(:in => cmd, :pipe => true, :log => true))
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
          f.puts "library(utils, quietly=TRUE);\n"
          f.puts "source('#{R::UTIL}');\n"
          f.puts 
          f.puts script
        end
        CMD.cmd("env R_PROFILE='#{init_file}' xterm \"$RHOME/bin/R\"")
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
    when "NA"
      "NA"
    when TSV
      "matrix(#{R.ruby2R object.values},dimnames=list(#{R.ruby2R object.keys}, #{R.ruby2R object.fields}))"
    when Symbol
      "#{ object }"
    when String
      object[0] == ":" ? object[1..-1] : "'#{ object }'"
    when Fixnum, Float
      object
    when TrueClass
      "TRUE"
    when FalseClass
      "FALSE"
    when Array
      "c(#{object.collect{|e| ruby2R(e) } * ", "})"
    when Hash
      object.collect{|k,v| [k, ruby2R(v)] * "="} * ", "
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

  def R(script, source = nil, open_options = {})
    open_options, source = source, nil if Hash === source

    source ||= Misc.process_options open_options, :source
    source = [source] unless Array === source 

    require_sources  = source.collect{|source|
      source = R::LIB_DIR["plot.R"] if source == :plot
      "source('#{source}')"
    } * ";\n" if Array === source and source.any?

    script = require_sources + "\n\n" + script if require_sources

    r_options = Misc.pull_keys open_options, :R
    r_options[:debug] = true if r_options[:method] == :debug
    if r_options.delete :debug
      r_options[:monitor] = true
      r_options[:method] = :shell
      erase = false
    else
      erase = true
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

      open_options = Misc.add_defaults open_options, :type => :list
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

  def R_interactive(pre_script = nil)
    TmpFile.with_file do |f|
      Log.debug{"R Interactive:\n" << pre_script } if pre_script
      TmpFile.with_file(pre_script) do |script_file|
        Open.write(f, self.to_s)
        script = "data_file = '#{f}';\n"
        script << "script_file = '#{script_file}';\n" if pre_script
        R.interactive(script)
      end
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
