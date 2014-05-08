module R
  def self.instance
    @@instance ||= begin
                     require 'rserve'
                     @@server_process = CMD.cmd('R CMD Rserve --vanilla', :pipe => true)
                     Rserve::Connection.new
                   end
  end

  def self.eval(cmd)
    instance.eval(cmd).payload.first
  end

  def self.eval(cmd)
    RSRuby.eval cmd
  end
end
