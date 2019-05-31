require 'rbbt/util/log'
require 'stringio'
require 'open3'

module CMD

  def self.gzip_pipe(file)
    Open.gzip?(file) ? "<(gunzip -c '#{file}')" : "'#{file}'"
  end

  def self.bash(cmd)
    %Q(bash <<EOF\n#{cmd}\nEOF\n)
  end

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

    sin, sout, serr, wait_thr = begin
                                  Open3.popen3(ENV, cmd)
                                rescue
                                  Log.warn $!.message
                                  raise ProcessFailed, cmd unless no_fail
                                  return
                                end
    pid = wait_thr.pid

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
          Log.error "Error in CMD  [#{pid}] #{cmd}: #{$!.message}"
          raise $!
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
          Log.log "STDERR [#{pid}]: " +  line, stderr 
        end if Integer === stderr and log
        serr.close
      end

      ConcurrentStream.setup sout, :pids => pids, :threads => [in_thread, err_thread, wait_thr].compact, :autojoin => no_wait, :no_fail => no_fail 

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

      status = wait_thr.value
      if not status.success? and not no_fail
        raise ProcessFailed.new "Command [#{pid}] #{cmd} failed with error status #{status.exitstatus}.\n#{err}"
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

    while c = io.getc
      STDERR << c if Log.severity == 0
      if c == "\n"
        if pid
          Log.logn "STDOUT [#{pid}]: ", 0
        else
          Log.logn "STDOUT: ", 0
        end
      end
    end 
    io.join

    nil
  end
end
