require 'rbbt/util/R'
require 'rserve'

# Hack to make it work with local sockets
module Rserve
  module TCPSocket 
    def self.new(hostname, port_number)
      raise "Socket at #{hostname} not found" unless File.exists? hostname
      @s = Socket.unix hostname
    end
  end
end

module R
  PID = Process.pid
  def self.instance
    @@instance ||= begin
                     @@socket_file = Rbbt.tmp.R_sockets[R::PID].find

                     FileUtils.mkdir_p File.dirname(@@socket_file) unless File.directory?(File.dirname(@@socket_file))

                     begin

                       if not File.exists? @@socket_file

                         @@instance_process = Process.fork do
                           args = %w(CMD Rserve --vanilla --quiet --RS-socket)
                           args << "'#{@@socket_file}'"

                           bin_path = "R"
                           cmd = bin_path + " " + args*" "
                           exec(ENV, cmd)
                         end
                         sleep 1
                       end

                       begin
                        i = Rserve::Connection.new :hostname => @@socket_file
                        i.eval "source('#{UTIL}');" 
                        i
                       rescue Exception
                         raise TryAgain
                       end
                     rescue Exception
                       Process.kill :INT, @@instance_process if defined? @@instance_process and @@instance_process
                       FileUtils.rm @@socket_file if File.exists? @@socket_file
                       retry if TryAgain === $!
                       raise $!
                     end
                   end
  end

  def self._eval(cmd)
    instance.eval(cmd)
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
