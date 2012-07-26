require 'rbbt/util/cmd'
require 'rbbt/util/misc'
require 'rbbt/util/tmpfile'

require 'zlib'

module Open
  class OpenURLError < StandardError; end
  class OpenGzipError < StandardError; end

  REMOTE_CACHEDIR = "/tmp/open_cache" 
  FileUtils.mkdir REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR

  def self.cachedir=(cachedir)
    REMOTE_CACHEDIR.replace cachedir
    FileUtils.mkdir REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR
  end

  def self.cachedir
    REMOTE_CACHEDIR
  end

  # Remote WGET
  LAST_TIME = {}
  def self.wait(lag, key = nil)
    time = Time.now   

    if LAST_TIME[key] != nil && (time < LAST_TIME[key] + lag)
      sleep (LAST_TIME[key] + lag) - time
    end

    LAST_TIME[key] = Time.now   
  end

  def self.wget(url, options = {})
    Log.low "WGET:\n -URL: #{ url }\n -OPTIONS: #{options.inspect}"
    options = Misc.add_defaults options, "--user-agent=" => 'firefox', :pipe => true

    wait(options[:nice], options[:nice_key]) if options[:nice]
    options.delete(:nice)
    options.delete(:nice_key)

    pipe  = options.delete(:pipe)
    quiet = options.delete(:quiet)
    post  = options.delete(:post)
    cookies = options.delete(:cookies)

    options["--quiet"]     = quiet if options["--quiet"].nil?
    options["--post-data="] ||= post if post

    if cookies
      options["--save-cookies"] = cookies
      options["--load-cookies"] = cookies
      options["--keep-session-cookies"] = true
    end


    stderr = case
             when options['stderr']
               options['stderr'] 
             when options['--quiet']
               false
             else
               nil
             end

    begin
      CMD.cmd("wget '#{ url }'", options.merge(
        '-O' => '-', 
        :pipe => pipe, 
        :stderr => stderr
      ))
    rescue
     STDERR.puts $!.backtrace.inspect
     raise OpenURLError, "Error reading remote url: #{ url }.\n#{$!.message}"
    end
  end

  def self.digest_url(url, options = {})
    params = [url, options.values_at("--post-data", "--post-data="), (options.include?("--post-file")? Open.read(options["--post-file"]).split("\n").sort * "\n" : "")]
    digest = Misc.digest(params.inspect)
  end
  # Cache
  #
  def self.in_cache(url, options = {})
    filename = File.join(REMOTE_CACHEDIR, digest_url(url, options))
    if File.exists? filename
      return filename 
    else
      nil
    end
  end
 
  def self.remove_from_cache(url, options = {})
    digest = Misc.digest([url, options.values_at("--post-data", "--post-data="), (options.include?("--post-file")? Open.read(options["--post-file"]) : "")].inspect)

    filename = File.join(REMOTE_CACHEDIR, digest)
    if File.exists? filename
      FileUtils.rm filename 
    else
      nil
    end
  end
  
  def self.add_cache(url, data, options = {})
    file = File.join(REMOTE_CACHEDIR, digest_url(url, options))
    Misc.sensiblewrite(file, data)
  end

  # Grep
  
  def self.grep(stream, grep)
    case 
    when Array === grep
      TmpFile.with_file(grep * "\n", false) do |f|
        CMD.cmd("grep", "-w" => true, "-f" => f, :in => stream, :pipe => true, :post => proc{FileUtils.rm f})
      end
    else
      CMD.cmd("grep '#{grep}' -", :in => stream, :pipe => true, :post => proc{stream.force_close if stream.respond_to? :force_close})
    end
  end
  
  def self.file_open(file, grep, mode = 'r')
    if grep
      grep(File.open(file, mode), grep)
    else
      File.open(file, mode)
    end
  end

  # Decompression
   
  def self.gunzip(stream)
    if String === stream
      Zlib::Inflate.inflate(stream)
    else
      CMD.cmd("gunzip", :pipe => true, :in => stream, :post => proc{stream.force_close if stream.respond_to? :force_close})
    end
  end

  def self.unzip(stream)
    TmpFile.with_file(stream.read) do |filename|
      StringIO.new(CMD.cmd("unzip '{opt}' #{filename}", "-p" => true, :pipe => true).read)
    end
  end

  # Questions

  def self.remote?(file)
    !! (file =~ /^(?:https?|ftp):\/\//)
  end

  def self.gzip?(file)
    !! (file =~ /\.gz$/)
  end

  def self.zip?(file)
    !! (file =~ /\.zip/)
  end


  # Open Read Write

  def self.open(url, options = {})
    options = Misc.add_defaults options, :noz => false, :mode => 'r'

    mode = Misc.process_options options, :mode

    wget_options = options[:wget_options] || {}
    wget_options[:nice] = options.delete(:nice)
    wget_options[:nice_key] = options.delete(:nice_key)
    wget_options[:quiet] = options.delete(:quiet)
    wget_options["--post-data="] = options.delete(:post) if options.include? :post
    wget_options["--post-file"] = options.delete("--post-file") if options.include? "--post-file"
    wget_options["--post-file="] = options.delete("--post-file=") if options.include? "--post-file="
    wget_options[:cookies] = options.delete(:cookies)

    io = case
         when (IO === url or StringIO === url)
           url
         when (not remote?(url))
           file_open(url, options[:grep], mode)
         when (options[:nocache] and options[:nocache] != :update)
           # What about grep?
           wget(url, wget_options)
         when (options[:nocache] != :update and in_cache(url, wget_options))
           Misc.lock(in_cache(url, wget_options)) do
             file_open(in_cache(url, wget_options), options[:grep], mode)
           end
         else
           io = wget(url, wget_options)
           add_cache(url, io, wget_options)
           io.close
           file_open(in_cache(url, wget_options), options[:grep], mode)
         end
    io = unzip(io)  if ((String === url and zip?(url))  and not options[:noz]) or options[:zip]
    io = gunzip(io) if ((String === url and gzip?(url)) and not options[:noz]) or options[:gzip]

    if block_given?
      yield io 
    else
      io
    end

    class << io;
      attr_accessor :filename
    end

    io.filename = url.to_s
    io
  end

  def self.can_open?(file)
    String === file and (File.exists?(file) or remote?(file))
  end

  def self.read(file, options = {}, &block)
    f = open(file, options)

    if block_given?
      res = []
      while not f.eof?
        l = f.gets
        l = Misc.fixutf8(l) 
        res << yield(l)
      end
      f.close
      res
    else
      text = Misc.fixutf8(f.read)
      f.close unless f.closed?
      text 
    end
  end

  def self.write(file, content = nil, options = {})
    options = Misc.add_defaults options, :mode => 'w'

    mode = Misc.process_options options, :mode

    FileUtils.mkdir_p File.dirname(file)
    case
    when content.nil?
      begin
        File.open(file, mode) do |f| 
          yield f
        end
      rescue Exception
        FileUtils.rm file if File.exists? file
        raise $!
      end
    when String === content
      File.open(file, mode) do |f|
        f.flock(File::LOCK_EX)
        f.write content 
        f.flock(File::LOCK_UN)
      end
    else
      begin
        File.open(file, mode) do |f| 
          f.flock(File::LOCK_EX)
          while not content.eof?
            f.write content.gets
          end
          f.flock(File::LOCK_UN)
        end
      rescue Exception
        FileUtils.rm file if File.exists? file
        raise $!
      end
      content.close
    end
  end
end

if __FILE__ == $0
  require 'benchmark'
  require 'progress-monitor'

  file = '/home/mvazquezg/rbbt/data/dbs/entrez/gene_info'
  puts Benchmark.measure {
    #Open.open(file).read.split(/\n/).each do |l| l end
    Open.read(file) do |l| l end
  }
end
