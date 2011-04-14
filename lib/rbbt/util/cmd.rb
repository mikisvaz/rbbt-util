require 'rbbt/util/misc'
require 'rbbt/util/log'
require 'stringio'

module CMD
  class CMDError < RBBTError; end

  module SmartIO 
    def self.tie(io, pid = nil, cmd = "",  post = nil)
      io.instance_eval{
        @pid  = pid
        @cmd  = cmd
        @post = post
        alias original_close close
        def close
          begin
            self.original_read unless self.closed? or self.eof?
            Process.waitpid(@pid) if @pid
          rescue
          end

          if $? and not $?.success?
            Log.debug "Raising exception"
            exception      = CMDError.new "Command [#{@pid}] #{@cmd} failed with error status #{$?.exitstatus}"
            original_close
            raise exception
          end

          @post.call if @post
          original_close
        end

        def force_close
          if @pid
            Log.debug "Forcing close by killing '#{@pid}'"
            Process.kill("KILL", @pid)
            Process.waitpid(@pid)
          end
          @post.call if @post
          original_close
        end
 
        alias original_read read
        def read
          data = Misc.fixutf8(original_read)
          self.close unless self.closed?
          data
        end

      }
      io
    end

 end

  def self.process_cmd_options(options = {})
    string = ""
    options.each do |option, value|
      case 
      when value.nil? || FalseClass === value 
        next
      when TrueClass === value
        string << "#{option} "
      else
        if option.chars.to_a.last == "="
          string << "#{option}#{value} "
        else
          string << "#{option} #{value} "
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

    if stderr == true
      stderr = Log::HIGH
    end

    # Process cmd_options
    cmd_options = process_cmd_options options
    if cmd =~ /'\{opt\}'/
      cmd.sub!('\'{opt}\'', cmd_options) 
    else
      cmd << " " << cmd_options
    end

    sout, serr = IO.pipe, IO.pipe

    case 
    when (false and (IO === in_content and not StringIO === in_content))
      sin = [in_content, nil]
    else 
      sin = IO.pipe
    end


    pid = fork {
      begin

        sin.last.close if sin.last
        STDIN.reopen sin.first
        sin.first.close

        serr.first.close
        STDERR.reopen serr.last
        serr.last.close

        sout.first.close
        STDOUT.reopen sout.last
        sout.last.close

        STDOUT.sync = STDERR.sync = true
        exec(cmd)
      rescue Exception
        raise CMDError, $!.message
      end
    }
    sin.first.close
    sout.last.close
    serr.last.close


    Log.debug "CMD: [#{pid}] #{cmd}"

    case 
    when String === in_content
      sin.last.write in_content
      sin.last.close
    when in_content.respond_to?(:gets)
      Thread.new do
        while not in_content.eof?
          sin.last.write in_content.gets
        end
        sin.last.close
        begin
          in_content.close
        rescue
          Process.kill "INT", pid
          raise $!
        end
      end
    end

    if pipe
      Thread.new do
        while l = serr.first.gets
          Log.log l, stderr if Integer === stderr
        end
        serr.first.close
      end

      SmartIO.tie sout.first, pid, cmd, post
      sout.first
    else
      err = ""
      Thread.new do
        while l = serr.first.gets
          err << l if Integer === stderr
        end
        serr.first.close
      end

      out = StringIO.new sout.first.read
      SmartIO.tie out, pid, cmd, post

      Process.waitpid pid

      if not $?.success?
        exception      = CMDError.new "Command [#{pid}] #{cmd} failed with error status #{$?.exitstatus}"
        exception.info = err if Integer === stderr and stderr >= Log.severity
        raise exception
      else
        Log.log err, stderr if Integer === stderr
      end

      out
    end
  end
end
