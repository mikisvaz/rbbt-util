require 'rbbt/util/log'
require 'stringio'

module CMD

  def self.process_cmd_options(options = {})
    string = ""
    options.each do |option, value|
      raise "Invalid option key: #{option.inspect}" if option.to_s !~ /^[a-z_0-9\-=]+$/i
      raise "Invalid option value: #{value.inspect}" if value.to_s.include? "'"

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

  def self.cmd(cmd, options = {}, &block)
    options = Misc.add_defaults options, :stderr => Log::DEBUG
    in_content = options.delete(:in)
    stderr     = options.delete(:stderr)
    pipe       = options.delete(:pipe)
    post       = options.delete(:post)
    log        = options.delete(:log)
    no_fail    = options.delete(:no_fail)
    no_wait    = options.delete(:no_wait)

    dont_close_in  = options.delete(:dont_close_in)

    log = true if log.nil?

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

    sout, serr, sin = Misc.pipe, Misc.pipe, Misc.pipe

    pid = fork {
      begin
        Misc.purge_pipes(sin.last,sout.last,serr.last)

        sin.last.close
        sout.first.close
        serr.first.close

        if IO === in_content
          in_content.close if in_content.respond_to?(:close) and not in_content.closed?
          in_content.join if in_content.respond_to?(:join) and not in_content.joined?
        end


        STDERR.reopen serr.last
        serr.last.close

        STDIN.reopen sin.first
        sin.first.close

        STDOUT.reopen sout.last
        sout.last.close

        STDOUT.sync = STDERR.sync = true

        exec(ENV, cmd)

        exit(-1)
      rescue Exception
        Log.debug{ "ProcessFailed: #{$!.message}" } if log
        Log.debug{ "Backtrace: \n" + $!.backtrace * "\n" } if log
        raise ProcessFailed, $!.message
      end
    }

    sin.first.close
    sout.last.close
    serr.last.close


    sin = sin.last
    sout = sout.first
    serr = serr.first


    Log.debug{"CMD: [#{pid}] #{cmd}" if log}

    if in_content.respond_to?(:read)
      in_thread = Thread.new(Thread.current) do |parent|
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
          parent.raise $!
          Process.kill "INT", pid
        ensure
          sin.close  unless sin.closed?
        end
      end
    else
      in_thread = nil
      sin.close
    end

    pids = [pid]

    if pipe
      err_thread = Thread.new do
        while line = serr.gets
          Log.log "STDERR [#{pid}]: " +  line, stderr if Integer === stderr and log
        end
        serr.close
      end

      ConcurrentStream.setup sout, :pids => pids, :threads => [in_thread, err_thread].compact, :autojoin => no_wait, :no_fail => no_fail 

      sout
    else
      err = ""
      err_thread = Thread.new do
        while not serr.eof?
          err << serr.gets if Integer === stderr
        end
        serr.close
      end

      ConcurrentStream.setup sout, :pids => pids, :threads => [in_thread, err_thread].compact, :autojoin => no_wait, :no_fail => no_fail 

      out = StringIO.new sout.read
      sout.close unless sout.closed?

      Process.waitpid pid

      if not $?.success? and not no_fail
        raise ProcessFailed.new "Command [#{pid}] #{cmd} failed with error status #{$?.exitstatus}.\n#{err}"
      else
        Log.log err, stderr if Integer === stderr and log
      end

      out
    end
  end

  def self.cmd_log(*args)
    all_args = *args

    all_args << {} unless Hash === all_args.last
    all_args.last[:log] = true
    all_args.last[:pipe] = true

    io = cmd(*all_args)
    pid = io.pids.first
    while line = io.gets
      if pid
        Log.debug "STDOUT [#{pid}]: " + line
      else
        Log.debug "STDOUT: " + line
      end
    end
    io.join
    nil
  end
end
