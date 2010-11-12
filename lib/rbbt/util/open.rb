require 'rbbt'
require 'rbbt/util/cmd'
require 'rbbt/util/misc'

require 'zlib'
require 'digest/md5'

module Open
  class OpenURLError < StandardError; end
  class OpenGzipError < StandardError; end

  REMOTE_CACHEDIR =  File.join(Rbbt.cachedir, 'open-remote/') unless defined? REMOTE_CACHEDIR
  FileUtils.mkdir REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR

  def self.cache(url, data = nil)
    digest = Digest::MD5.hexdigest(url)

    Misc.sensiblewrite(File.join(REMOTE_CACHEDIR, digest), data) if data
    if File.exist? File.join(REMOTE_CACHEDIR, digest)
      File.open(File.join(REMOTE_CACHEDIR, digest))
    else
      return nil
    end
  end

  LAST_TIME = {}
  def self.wait(lag, key = nil)
    time = Time.now   

    if LAST_TIME[key] != nil && (time < LAST_TIME[key] + lag)
      sleep (LAST_TIME[key] + lag + 0.5) - time
    end

    LAST_TIME[key] = Time.now   
  end

  def self.wget(url, options = {})
    options = Misc.add_defaults options, "--user-agent=" => 'firefox'

    wait(options[:nice], options[:nice_key]) if options[:nice]
    options.delete(:nice)
    options.delete(:nice_key)

    begin
      CMD.cmd("wget '#{ url }'", options.merge('-O' => '-'))
    rescue
      raise OpenURLError, "Error reading remote url: #{ url }"
    end
  end

  def self.remote?(file)
    (file =~ /^(?:https?|ftp):\/\//) != nil
  end

  def self.gzip?(file)
    (file =~ /\.gz$/) != nil
  end

  def self.zip?(file)
    (file =~ /\.zip/) != nil
  end

  def self.gunzip(stream)
    Zlib::Inflate.inflate(stream)
  end

  def self.unzip(stream)
    CMD.cmd('zip /dev/stdin', "-p" => true, :in => stream)
  end

  def self.open_remote?(file, options = {})
    if remote? file
      wget(file, "--user-agent" => "firefox", "-q" => options[:quiet], :nice => options[:nice])
    else
      File.open(file)
    end
  end

  def self.open_compressed?(file, options = {})
    case
    when gzip?(file) || options[:gzip]
      gunzip(open_remote?(file, options))
    when zip?(file) || options[:zip]
      unzip(open_remote?(file, options))
    else
      open_remote?(file, options)
    end
  end

  def self.open_cache?(file, options = {})
    options = Misc.add_defaults :quite => false, :nocache => false, :nice => nil

    f = nil
    case
    when options[:nocache] || ! remote?(file)
      open_compressed?(file, options)
    when cache(file)
      cache(file)
    else
      cache(file, open_compressed?(file, options))
    end
  end

  class << self
    alias_method :open,  :open_cache?
  end

  def self.read(file)
    if block_given?
      f = open(file)
      while ! f.eof?
        yield f.readline
      end
    else
      open(file).read
    end
  end
end
