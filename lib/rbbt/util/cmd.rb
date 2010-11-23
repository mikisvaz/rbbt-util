require 'open4'
require 'rbbt/util/misc'
require 'stringio'

module CMD
  class CMDError < StandardError;end

  module SmartIO 
    def self.tie_pmid(io, pid, post = nil)
      io.instance_eval{
        @pid  = pid
        @post = post
        alias original_close close
        def close
          Process.waitpid(@pid, Process::WNOHANG)
          @post.call if @post
          original_close
        end

        alias original_read read
        def read
          data = Misc.fixutf8(original_read)
          self.close
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
    in_content = options.delete(:in)
    stderr     = options.delete(:stderr)
    pipe       = options.delete(:pipe)
    post       = options.delete(:post)

    # Process cmd_options
    cmd_options = process_cmd_options options
    if cmd =~ /'\{opt\}'/
      cmd.sub!('\'{opt}\'', cmd_options) 
    else
      cmd << " " << cmd_options
    end

    sout, serr = IO.pipe, IO.pipe

    case 
    when (IO === in_content and not StringIO === in_content)
      sin = [in_content, nil]
    else StringIO === in_content
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

        exec(cmd)
      rescue Exception
        STDERR.puts $!.message
      end

    }
    sin.first.close
    sout.last.close
    serr.last.close

    case 
    when String === in_content
      sin.last.write in_content
      sin.last.close
    when StringIO === in_content
      Thread.new do
        while l = in_content.gets
          sin.last.write l
        end
        sin.last.close
      end
    end

    Thread.new do
      while l = serr.first.gets
        STDERR.puts l if stderr
      end
      serr.first.close
    end

    if pipe
      SmartIO.tie_pmid sout.first, pid, post
      sout.first
    else
      out = StringIO.new sout.first.read
      Process.wait pid
      out
    end
  end

  def self.cmd2(cmd, options = {}, &block)
    in_content = options.delete(:in)
    pipe       = options.delete(:pipe)
    stderr     = options.delete(:stderr)
    post       = options.delete(:post)

    # Process cmd_options
    cmd_options = process_cmd_options options
    if cmd =~ /'\{opt\}'/
      cmd.sub!('\'{opt}\'', cmd_options) 
    else
      cmd << " " << cmd_options
    end

    # Use block if given
    if block_given?
      status = Open4.popen4(cmd) &block
      raise CMDError if ! status.success?
      return
    end

    begin

      # Input stream
      case
      when in_content.nil?
        pid, sin, sout, serr = Open4.open4 cmd
        sin.close
      when String === in_content
        pid, sin, sout, serr = Open4.open4 cmd
        sin.write in_content
        sin.close
      when IO === in_content
        Thread.new do
          while l = in_content.gets
            sin.write l
          end
          sin.close
          in_content.close
        end
      end

      if pipe
        sout.extend SmartIO
        SmartIO.tie_pmid sout, pid, post
        return sout
      else
        if stderr
          Thread.new do
            begin
              while l = serr.gets
                STDERR.puts l
              end
              serr.close
            rescue
              retry
            end
          end
        else
          serr.close
        end

        out = sout.read
        sout.close
        Process.waitpid(pid)
        raise StandardError, serr.read if $? && ! $?.success?
        sout = StringIO.new Misc.fixutf8(out)
        return sout
      end

    rescue StandardError
      out = sout.read unless sout.nil? or sout.closed?
      out ||= ""
      raise CMDError, ["","---","- Message(STDERR): #{$!.message}", "- Backtrace: #{$!.backtrace * ";;" }", "- STDOUT:\n#{ out[1..100] }", "---"] * "\n" 
    end
  end
end
