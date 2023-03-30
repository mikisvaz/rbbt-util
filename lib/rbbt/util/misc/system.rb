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

  def self.env_add(var, value, sep = ":", prepend = true)
    ENV[var] ||= ""
    return if ENV[var] =~ /(#{sep}|^)#{Regexp.quote value}(#{sep}|$)/
      if prepend
        ENV[var] = value + sep + ENV[var]
      else
        ENV[var] += sep + ENV[var]
      end
  end

  def self.with_env(var, value, &block)
    var = var.to_s
    value = value.to_s
    current = ENV[var]
    begin
      ENV[var] = value
      yield
    ensure
      ENV[var] = current
    end
  end

  def self.path_relative_to(basedir, path)
    path = File.expand_path(path) unless path.slice(0,1) == "/"
    basedir = File.expand_path(basedir) unless basedir.slice(0,1) == "/"

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


  def self.relative_link(source, target_dir)
    path = "."
    current = target_dir
    while ! Misc.common_path current, source
      current = File.dirname(current)
      path = File.join(path, '..')
      return nil if current == "/"
    end

    File.join(path, Misc.path_relative_to(current, source))
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

  def self.is_filename?(string, need_to_exists = true)
    return false if string.nil?
    return true if defined? Path and Path === string
    return true if string.respond_to? :exists
    return true if String === string and ! string.include?("\n") and string.split("/").select{|p| p.length > 265}.empty? and (! need_to_exists || File.exist?(string))
    return false
  end

  class << self
    alias filename? is_filename?
  end
end
