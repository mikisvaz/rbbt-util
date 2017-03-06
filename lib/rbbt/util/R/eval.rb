require 'rbbt/util/R'
require 'rserve'

# Hack to make it work with local sockets
module Rserve
  module TCPSocket 
    def self.new(hostname, port_number)
      raise "Socket at #{hostname} not found" unless File.exist? hostname
      @s = Socket.unix hostname
    end
  end
end

module R
  SESSION = ENV["RServeSession"] || "Session-PID-" + Process.pid.to_s

  def self.socket_file
    @@socket_file ||= Rbbt.tmp.R_sockets[R::SESSION].find
  end

  def self.lockfile
    @@lockfile ||= socket_file + '.lock'
  end

  def self.semfile
    if defined? @@semfile and not @@semfile.nil?
      @@semfile 
    else
      @@semfile = File.basename(socket_file) + '.sem'
      RbbtSemaphore.create_semaphore(@@semfile,1) 
      @@semfile
    end
  end

  def self.workdir
    @@workdir ||= socket_file + '.wd'
  end

  def self.pid_file
    @@pidfile ||= File.join(workdir, 'pid')
  end

  def self.clear
    @@instance = nil
    if defined? @@instance_process and @@instance_process and Misc.pid_exists? @@instance_process
      Log.warn "Clearing Rserver session #{SESSION}, PID #{@@instance_process}"
      begin
        Process.kill :INT, @@instance_process
      rescue Exception
        Log.warn "Error killing Rserve (#{@@instance_process}): #{$!.message}"
      end
    end
    FileUtils.rm_rf pid_file if File.exist? pid_file
    FileUtils.rm_rf socket_file if File.exist? socket_file
    FileUtils.rm_rf lockfile if File.exist? lockfile
    FileUtils.rm_rf workdir if File.exist? workdir
  end

  def self.instance
    @@instance ||= begin

                     clear if File.exist? pid_file and ! Misc.pid_exists?(Open.read(pid_file).strip.to_i)

                     FileUtils.mkdir_p File.dirname(socket_file) unless File.directory?(File.dirname(socket_file))
                     FileUtils.mkdir_p workdir unless File.directory? workdir

                     at_exit do
                       self.clear
                     end unless defined? @@instance_process

                     begin

                       if not File.exist? socket_file

                         sh_pid = Process.fork do
                           #args = %w(CMD Rserve --vanilla --quiet --RS-socket)
                           args = %w(--quiet --no-save --RS-socket)
                           args << "'#{socket_file}'"
                           args << "--RS-workdir"
                           args << "'#{workdir}'"
                           args << "--RS-pidfile"
                           args << "'#{pid_file}'"

                           if ENV["R_HOME"]
                             bin_path = File.join(ENV["R_HOME"], "bin/Rserve") 
                           else
                             bin_path = "Rserve"
                           end
                           cmd = bin_path + " " + args*" "
                           $stdout.reopen File.new('/dev/null', 'w')
                           exec(ENV, cmd)
                         end
                         while not File.exist? pid_file
                           sleep 0.5
                         end
                         @@instance_process = Open.read(pid_file).to_i
                         Log.info "New Rserver session stated with PID (#{sh_pid}) #{@@instance_process}: #{SESSION}"
                       end

                       i = Rserve::Connection.new :hostname => socket_file

                       begin
                        FileUtils.mkdir workdir unless File.exist? workdir
                        i.eval "setwd('#{workdir}');"
                        i.eval "source('#{UTIL}');" 
                        i
                       rescue Exception
                         Log.exception $!
                         raise TryAgain
                       end
                     rescue Exception
                       Log.exception $!
                       Process.kill :INT, @@instance_process if defined? @@instance_process and @@instance_process
                       FileUtils.rm socket_file if File.exist? socket_file
                       retry if TryAgain === $!
                       raise $!
                     end
                   end
  end

  def self._eval(cmd)
    RbbtSemaphore.synchronize(semfile) do 
      times = 1
      begin
        instance.eval(cmd)
      rescue Rserve::Connection::EvalError
        times = times - 1
        if times > 0
          clear
          retry 
        else
          raise $!
        end
      end
    end
  end

  def self.eval_a(cmd)
    _eval(cmd).payload
  end

  def self.eval(cmd)
    eval_a(cmd).first
  end

  def self.eval_run(cmd)
    _eval(cmd)
  end

end
