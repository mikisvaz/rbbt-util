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

  def rbbt(script)
    preamble =<<-EOF
require 'rbbt-util'
require 'rbbt/workflow'
    EOF

    ruby(preamble + "\n" + script)
  end

  def workflow(workflow, script)
    preamble =<<-EOF
wf = Workflow.require_workflow('#{workflow}')
    EOF

    rbbt(preamble + "\n" + script)
  end

  @connections = {}
  def self.open(host, user = nil)
    @connections[[host, user]] ||= SSHLine.new host, user
  end

  def self.run(server, cmd, options = nil)
    cmd = cmd * " " if Array === cmd
    cmd += " " + CMD.process_cmd_options(options) if options
    open(server).cmd(cmd)
  end

  def self.ruby(server, script)
    open(server).ruby(script)
  end

  def self.rbbt(server, script)
    open(server).rbbt(script)
  end

  def self.workflow(server, workflow, script)
    open(server).workflow(workflow, script)
  end



  def self.command(server, command, argv = [], options = nil)
    run(server, [command] + argv, options)
  end
end
