require 'rbbt/util/cmd'
require 'rbbt/util/misc'
require 'rbbt/util/tmpfile'

require 'zlib'
require 'digest/md5'

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
    options = Misc.add_defaults options, "--user-agent=" => 'firefox', :pipe => true

    wait(options[:nice], options[:nice_key]) if options[:nice]
    options.delete(:nice)
    options.delete(:nice_key)

    pipe  = options.delete(:pipe)
    quiet = options.delete(:quiet)
    options["--quiet"] = quiet if options["--quiet"].nil?

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

  # Cache

  def self.in_cache(url, options = {})
    digest = Digest::MD5.hexdigest(url)

    filename = File.join(REMOTE_CACHEDIR, digest)
    if File.exists? filename
      return filename 
    else
      nil
    end
  end
  
  def self.add_cache(url, data, options = {})
    digest = Digest::MD5.hexdigest(url)
    Misc.sensiblewrite(File.join(REMOTE_CACHEDIR, digest), data)
  end

  # Grep
  
  def self.grep(stream, grep)
    case 
    when Array === grep
      TmpFile.with_file(grep * "\n", false) do |f|
        CMD.cmd("grep", "-F" => true, "-f" => f, :in => stream, :pipe => true, :post => proc{FileUtils.rm f})
      end
    else
      CMD.cmd("grep '#{grep}' -", :in => stream, :pipe => true)
    end
  end
  
  def self.file_open(file, grep)
    if grep
      grep(File.open(file), grep)
    else
      File.open(file)
    end
  end

  # Decompression
   
  def self.gunzip(stream)
    if String === stream
      Zlib::Inflate.inflate(stream)
    else
      CMD.cmd("gunzip", :pipe => true, :in => stream)
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
    options = Misc.add_defaults options, :noz => false

    wget_options = options[:wget_options] || {}
    wget_options[:nice] = options.delete(:nice)
    wget_options[:nice_key] = options.delete(:nice_key)

    io = case
         when (not remote?(url))
           file_open(url, options[:grep])
         when options[:nocache]
           wget(url, wget_options)
         when in_cache(url)
           file_open(in_cache(url), options[:grep])
         else
           io = wget(url, wget_options)
           add_cache(url, io)
           io.close
           file_open(in_cache(url), options[:grep])
         end
    io = unzip(io)  if zip?  url and not options[:noz]
    io = gunzip(io) if gzip? url and not options[:noz]

    io
  end

  def self.read(file, options = {}, &block)
    f = open(file, options)

    if block_given?
      while l = Misc.fixutf8(f.gets)
        l = fixutf8(l) if l.respond_to?(:valid_encoding?) && ! l.valid_encoding?
        yield l
      end
    else
      Misc.fixutf8(f.read)
    end
  end

  def self.write(file, content)
    if String === content
      File.open(file, 'w') do |f| f.write content end
    else
      File.open(file, 'w') do |f| 
        while l = content.gets
          f.write l
        end
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
