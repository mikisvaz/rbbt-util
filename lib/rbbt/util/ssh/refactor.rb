
class SSHLine
  def ruby(script)
    @output = ""
    @complete_output = false
    script = "require 'rbbt-util'\n" << script
    cmd = "ruby -I ~/git/rbbt6/lib/ -e \"#{script.gsub('"','\\"')}\"\n"
    Log.debug "Running ruby on #{@host}:\n#{ script }"
    @ch.send_data(cmd)
    @ch.send_data("echo DONECMD: $?\n")
    @ssh.loop{ !@complete_output }
    if @exit_status.to_i == 0
      return @output
    else
      raise SSHProcessFailed.new @host, "Ruby script:\n#{script}"
    end
  end
end

