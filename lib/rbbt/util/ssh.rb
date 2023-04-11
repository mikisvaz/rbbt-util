require 'net/ssh'

class SSHLine

  def initialize(host, user = nil)
    @host = host
    @user = user

    @ssh = Net::SSH.start(@host, @user)

    @ch = @ssh.open_channel do |ch|
      ch.exec 'bash'
    end

    @ch.on_data do |_,data|
      if m = data.match(/DONECMD: (\d+)\n/)
        @exit_status = m[1].to_i
        @output << data.sub(m[0],'')
        serve_output 
      else
        @output << data
      end
    end

    @ch.on_extended_data do |_,c,err|
      STDERR.write err 
    end
  end

  def send_cmd(command)
    @output = ""
    @complete_output = false
    @ch.send_data(command+"\necho DONECMD: $?\n")
  end

  def serve_output
    @complete_output = true
  end

  def cmd(command)
    send_cmd(command)
    @ssh.loop{ ! @complete_output}
    if @exit_status.to_i == 0
      return @output
    else
      raise SSHProcessFailed.new @host, command
    end
  end

  def ruby(script)
    @output = ""
    @complete_output = false
    cmd = "ruby -e \"#{script.gsub('"','\\"')}\"\n"
    @ch.send_data(cmd)
    @ch.send_data("echo DONECMD: $?\n")
    @ssh.loop{ !@complete_output }
    if @exit_status.to_i == 0
      return @output
    else
      raise SSHProcessFailed.new @host, "Ruby script:\n#{script}"
    end
  end

  @connections = {}
  def self.open(host, user = nil)
    @connections[[host, user]] ||= SSHLine.new host, user
  end

  def self.run(server, cmd)
    open(server).cmd(cmd)
  end

  def self.ruby(server, script)
    open(server).ruby(script)
  end
end
