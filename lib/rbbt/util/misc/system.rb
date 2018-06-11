require 'net/smtp'

module Misc

  def self.hostname
    @hostanem ||= `hostname`.strip
  end

  def self.pid_exists?(pid)
    return false if pid.nil?
    begin
      Process.getpgid(pid.to_i)
      true
    rescue Errno::ESRCH
      false
    end
  end

  def self.send_email(from, to, subject, message, options = {})
    IndiferentHash.setup(options)
    options = Misc.add_defaults options, :from_alias => nil, :to_alias => nil, :server => 'localhost', :port => 25, :user => nil, :pass => nil, :auth => :login

    server, port, user, pass, from_alias, to_alias, auth = Misc.process_options options, :server, :port, :user, :pass, :from_alias, :to_alias, :auth

    msg = <<-END_OF_MESSAGE
From: #{from_alias} <#{from}>
To: #{to_alias} <#{to}>
Subject: #{subject}

#{message}
END_OF_MESSAGE

Net::SMTP.start(server, port, server, user, pass, auth) do |smtp|
  smtp.send_message msg, from, to
end
  end

  def self.env_add(var, value, sep = ":", prepend = true)
    ENV[var] ||= ""
    return if ENV[var] =~ /(#{sep}|^)#{Regexp.quote value}(#{sep}|$)/
      if prepend
        ENV[var] = value + sep + ENV[var]
      else
        ENV[var] += sep + ENV[var]
      end
  end

  def self.path_relative_to(basedir, path)
    path = File.expand_path(path) unless path[0] == "/"
    basedir = File.expand_path(basedir) unless basedir[0] == "/"

    if path.index(basedir) == 0
      if basedir[-1] == "/"
        return path[basedir.length..-1]
      else
        return path[basedir.length+1..-1]
      end
    else
      return nil
    end
  end

  def self.common_path(dir, file)
    file = File.expand_path file
    dir = File.expand_path dir

    return true if file == dir
    while File.dirname(file) != file
      file = File.dirname(file)
      return true if file == dir
    end

    return false
  end

  # WARN: probably not thread safe...
  def self.in_dir(dir)
    old_pwd = FileUtils.pwd
    res = nil
    begin
      FileUtils.mkdir_p dir unless File.exist?(dir)
      FileUtils.cd dir
      res = yield
    ensure
      FileUtils.cd old_pwd
    end
    res
  end

  def self.filename?(string)
    String === string and string.length > 0 and string.length < 250 and File.exist?(string)
  end

  def self.is_filename?(string)
    return true if defined? PATH and Path === string
    return true if string.respond_to? :exists
    return true if String === string and string.length < 265 and File.exist? string
    return false
  end

end
