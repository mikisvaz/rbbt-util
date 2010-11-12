require 'open4'

module CMD
  class CMDError < StandardError;end

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
    in_content = options[:in]
    options.delete(:in)
    cmd_options = process_cmd_options options
    
    if cmd =~ /'\{opt\}'/
      cmd.sub!('\'{opt}\'', cmd_options) 
    else
      cmd << " " << cmd_options
    end

    case
    when block_given?
      status = Open4.popen4(cmd) &block
      raise CMDError if ! status.success?
    when in_content
      begin
        pid, sin, sout, serr = Open4.open4 cmd
        sin.write in_content 
        sin.close
        Process.waitpid(pid, Process::WNOHANG)
        raise StandardError if $? && ! $?.success?
        sout
      rescue StandardError
        puts $!.message
        raise CMDError, ["","---", "- STDOUT:\n#{ sout.read }", "- STDERR:\n#{serr.read}", "---"] * "\n" 
      end
    else
      begin
        pid, sin, sout, serr = Open4.open4 cmd
        sin.close
        Process.waitpid(pid)
        raise StandardError if $? && ! $?.success?
        serr.close
        sout
      rescue StandardError
        puts $!.message
        raise CMDError, ["","---", "- STDOUT:\n#{ sout.read }", "- STDERR:\n#{serr.read}", "---"] * "\n" 
      end

    end
  end
end
