require 'rbbt/util/misc'
require 'stringio'

module CMD
  class CMDError < StandardError;end

  module SmartIO 
    def self.tie(io, pid = nil, post = nil)
      io.instance_eval{
        @pid  = pid
        @post = post
        alias original_close close
        def close
          begin
            Process.waitpid(@pid, Process::WNOHANG) if @pid
          rescue
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
    options = Misc.add_defaults options, :stderr => true
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

        STDOUT.sync = STDERR.sync = true
        exec(cmd)
      rescue Exception
        STDERR.puts $!.message
        STDERR.puts $!.backtrace * "\n"
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
      SmartIO.tie sout.first, pid, post
      sout.first
    else
      out = StringIO.new sout.first.read
      SmartIO.tie out
      Process.waitpid pid
      out
    end
  end
end
