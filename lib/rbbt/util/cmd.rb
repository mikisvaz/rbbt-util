require 'rbbt/util/log'
require 'stringio'
require 'open3'
require 'rbbt/util/misc/indiferent_hash'

module CMD

  TOOLS = IndiferentHash.setup({})
  def self.tool(tool, claim = nil, test = nil, cmd = nil, &block)
    TOOLS[tool] = [claim, test, block, cmd]
  end

  def self.conda(tool, env = nil, channel = 'bioconda')
    if env
      CMD.cmd("bash -l -c '(conda activate #{env} && conda install #{tool} -c #{channel})'")      
    else
      CMD.cmd("bash -l -c 'conda install #{tool} -c #{channel}'")      
    end
  end

  def self.get_tool(tool)
    return tool.to_s unless TOOLS[tool]

    @@init_cmd_tool ||= IndiferentHash.setup({})

    claim, test, block, cmd = TOOLS[tool]
    cmd = tool.to_s if cmd.nil?

    if !@@init_cmd_tool[tool]

      begin
        if test
          CMD.cmd(test + " ")
        else
          CMD.cmd("#{cmd} --help")
        end
      rescue
        if claim
          claim.produce
        else
          block.call
        end
      end
      version_txt = ""
      version = nil
      ["--version", "-version", "--help", ""].each do |f|
        begin
          version_txt += CMD.cmd("#{cmd} #{f} 2>&1", :nofail => true).read
          version = Misc.scan_version_text(version_txt, tool)
          break if version
        rescue
          Log.exception $!
        end
      end

      @@init_cmd_tool[tool] = version || true

      return cmd if cmd
    end

    cmd
  end

  def self.versions
    return {} unless defined? @@init_cmd_tool
    @@init_cmd_tool.select{|k,v| v =~ /\d+\./ }
  end

  def self.gzip_pipe(file)
    Open.gzip?(file) ? "<(gunzip -c '#{file}')" : "'#{file}'"
  end

  def self.bash(cmd)
    %Q(bash <<EOF\n#{cmd}\nEOF\n)
  end

  def self.process_cmd_options(options = {})
    add_dashes = Misc.process_options options, :add_option_dashes

    string = ""
    options.each do |option, value|
      raise "Invalid option key: #{option.inspect}" if option.to_s !~ /^[a-z_0-9\-=.]+$/i
      #raise "Invalid option value: #{value.inspect}" if value.to_s.include? "'"
      value = value.gsub("'","\\'") if value.to_s.include? "'"

      option = "--" << option.to_s if add_dashes and option.to_s[0] != '-'

      case 
      when value.nil? || FalseClass === value 
        next
      when TrueClass === value
        string << "#{option} "
      else
        if option.to_s.chars.to_a.last == "="
          string << "#{option}'#{value}' "
        else
          string << "#{option} '#{value}' "
        end
      end
    end

    string.strip
  end

  def self.cmd(tool, cmd = nil, options = {}, &block)
    options, cmd = cmd, nil if Hash === cmd

    options = Misc.add_defaults options, :stderr => Log::DEBUG
    in_content = options.delete(:in)
    stderr     = options.delete(:stderr)
    pipe       = options.delete(:pipe)
    post       = options.delete(:post)
    log        = options.delete(:log)
    no_fail    = options.delete(:no_fail)
    no_fail    = options.delete(:nofail) if no_fail.nil?
    no_wait    = options.delete(:no_wait)
    xvfb       = options.delete(:xvfb)
    bar        = options.delete(:progress_bar)
    save_stderr = options.delete(:save_stderr)

    dont_close_in  = options.delete(:dont_close_in)

    log = true if log.nil?
    
    if cmd.nil? && ! Symbol === tool 
      cmd = tool
    else
      tool = get_tool(tool)
      if cmd.nil?
        cmd = tool
      else
        cmd = tool + ' ' + cmd
      end

    end

    case xvfb
    when TrueClass
      cmd = "xvfb-run --server-args='-screen 0 1024x768x24' --auto-servernum #{cmd}"
    when String
      cmd = "xvfb-run --server-args='#{xvfb}' --auto-servernum --server-num=1 #{cmd}"
    when String
    end

    if stderr == true
      stderr = Log::HIGH
    end

    cmd_options = process_cmd_options options
    if cmd =~ /'\{opt\}'/
      cmd.sub!('\'{opt}\'', cmd_options) 
    else
      cmd << " " << cmd_options
    end

    in_content = StringIO.new in_content if String === in_content

    sin, sout, serr, wait_thr = begin
                                  Open3.popen3(ENV, cmd)
                                rescue
                                  Log.warn $!.message
                                  raise ProcessFailed, nil, cmd unless no_fail
                                  return
                                end
    pid = wait_thr.pid

    Log.debug{"CMD: [#{pid}] #{cmd}" if log}

    if in_content.respond_to?(:read)
      in_thread = Thread.new(Thread.current) do |parent|
        Thread.current.report_on_exception = false if no_fail
        begin
          begin
            while c = in_content.readpartial(Misc::BLOCK_SIZE)
              sin << c 
            end
          rescue EOFError
          end
          sin.close  unless sin.closed?

          unless dont_close_in
            in_content.close unless in_content.closed? 
            in_content.join if in_content.respond_to? :join 
          end
        rescue
          Log.error "Error in CMD  [#{pid}] #{cmd}: #{$!.message}" unless no_fail
          raise $!
        end
      end
    else
      in_thread = nil
      sin.close
    end

    pids = [pid]

    if pipe

      ConcurrentStream.setup sout, :pids => pids, :autojoin => no_wait, :no_fail => no_fail 

      if (Integer === stderr and log) || bar
        err_thread = Thread.new do
          while line = serr.gets
            bar.process(line) if bar
            sout.log = line
            sout.std_err << line if save_stderr
            Log.log "STDERR [#{pid}]: " +  line, stderr if log
          end 
          serr.close
        end
      else
        err_thread = Misc.consume_stream(serr, true)
      end

      sout.threads = [in_thread, err_thread, wait_thr].compact

      sout
    else

      if bar
        err = ""
        err_thread = Thread.new do
          while not serr.eof?
            line = serr.gets 
            bar.process(line) 
            err << line if Integer === stderr and log
          end
          serr.close
        end
      elsif log and Integer === stderr
        err = ""
        err_thread = Thread.new do
          while not serr.eof?
            err << serr.gets 
          end
          serr.close
        end
      else
        Misc.consume_stream(serr, true)
        #serr.close 
        err_thread = nil
        err = ""
      end

      ConcurrentStream.setup sout, :pids => pids, :threads => [in_thread, err_thread].compact, :autojoin => no_wait, :no_fail => no_fail 

      out = StringIO.new sout.read
      sout.close unless sout.closed?

      status = wait_thr.value
      if not status.success? and not no_fail
        if !err.empty?
          raise ProcessFailed.new pid, "#{cmd} failed with error status #{status.exitstatus}.\n#{err}"
        else
          raise ProcessFailed.new pid, "#{cmd} failed with error status #{status.exitstatus}"
        end
      else
        Log.log err, stderr if Integer === stderr and log
      end

      out
    end
  end

  def self.cmd_pid(*args)
    all_args = *args

    bar = all_args.last[:progress_bar] if Hash === all_args.last

    all_args << {} unless Hash === all_args.last

    level = all_args.last[:log] || 0
    level = 0 if TrueClass === level
    level = 10 if FalseClass === level
    level = level.to_i

    all_args.last[:log] = true
    all_args.last[:pipe] = true

    io = cmd(*all_args)
    pid = io.pids.first

    line = "" if bar
    starting = true
    while c = io.getc
      if starting
        if pid
          Log.logn "STDOUT [#{pid}]: ", level
        else
          Log.logn "STDOUT: ", level
        end
        starting = false
      end
      STDERR << c if Log.severity <= level
      line << c if bar
      if c == "\n"
        bar.process(line) if bar
        starting = true
        line = "" if bar
      end
    end 
    begin
      io.join
      bar.remove if bar
    rescue
      bar.remove(true) if bar
      raise $!
    end

    nil
  end

  def self.cmd_log(*args)
    cmd_pid(*args)
    nil
  end

end
