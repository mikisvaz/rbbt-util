module RbbtSSH

  def self.ssh(server, argv = nil, options = {})
    server = server.sub(%r(^ssh:(//)?), '')

    argv = [] if argv.nil?
    argv = [argv] unless Array === argv

    options = Misc.add_defaults options, :add_option_dashes => true

    cmd_sections = [server]
    cmd_sections << argv * " "
    cmd_sections << CMD.process_cmd_options(options)

    cmd = cmd_sections.compact * " "

    CMD.cmd(:ssh, cmd, :pipe => true)
  end

  def self.command(server, command, argv = [], options = nil)
    ssh(server, [command] + argv, options)
  end

end

