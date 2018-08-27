require 'rbbt/util/cmd'
require 'rbbt/util/tmpfile'
require 'rbbt/util/misc'
require 'rbbt/util/misc/bgzf'

require 'zlib'

module Open
  class OpenURLError < StandardError; end
  class OpenGzipError < StandardError; end

  REMOTE_CACHEDIR = File.join(ENV["HOME"], "/tmp/open_cache")
  FileUtils.mkdir_p REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR

  class << self
    attr_accessor :repository_dirs

    def repository_dirs
      @repository_dirs ||= begin
                             File.exist?(Rbbt.etc.repository_dirs.find) ? 
                               File.read(Rbbt.etc.repository_dirs.find).split("\n") :
                               []
                           rescue
                             []
                           end
    end

  end

  def self.cachedir=(cachedir)
    REMOTE_CACHEDIR.replace cachedir
    FileUtils.mkdir_p REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR
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
    options = Misc.add_defaults options, "--user-agent=" => 'rbbt', :pipe => true

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
      wget_options = options.merge( '-O' => '-')
      wget_options[:pipe] = pipe unless pipe.nil?
      wget_options[:stderr] = stderr unless stderr.nil?

      CMD.cmd("wget '#{ url }'", wget_options)
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
    if File.exist? filename
      return filename 
    else
      nil
    end
  end
 
  def self.remove_from_cache(url, options = {})
    digest = Misc.digest([url, options.values_at("--post-data", "--post-data="), (options.include?("--post-file")? Open.read(options["--post-file"]) : "")].inspect)

    filename = File.join(REMOTE_CACHEDIR, digest)
    if File.exist? filename
      FileUtils.rm filename 
    else
      nil
    end
  end
  
  def self.add_cache(url, data, options = {})
    file = File.join(REMOTE_CACHEDIR, digest_url(url, options))
    Misc.sensiblewrite(file, data, :force => true)
  end

  # Grep
  
  def self.grep(stream, grep, invert = false)
    grep_cmd = ENV["GREP_CMD"] || "/bin/grep"
    case 
    when Array === grep
      TmpFile.with_file(grep * "\n", false) do |f|
        CMD.cmd("#{grep_cmd} #{invert ? '-v' : ''} -", "-w" => true, "-F" => true, "-f" => f, :in => stream, :pipe => true, :post => proc{FileUtils.rm f})
      end
    else
      CMD.cmd("#{grep_cmd} #{invert ? '-v ' : ''} '#{grep}' -", :in => stream, :pipe => true, :post => proc{begin stream.force_close; rescue Exception; end if stream.respond_to?(:force_close)})
    end
  end

  def self.get_repo_from_dir(dir)
    @repos ||= {}
    @repos[dir] ||= begin
                      repo_path = File.join(dir, '.file_repo')
                      Persist.open_tokyocabinet(repo_path, false, :clean,TokyoCabinet::BDB )
                    end
  end

  def self.get_stream_from_repo(dir, sub_path)
    repo = get_repo_from_dir(dir)
    repo.read_and_close do
      content = repo[sub_path]
      content.nil? ? nil : StringIO.new(content)
    end
  end

  def self.save_content_in_repo(dir, sub_path, content)
    repo = get_repo_from_dir(dir)
    repo.write_and_close do
      repo[sub_path] = content
    end
  end

  def self.remove_from_repo(dir, sub_path, recursive = false)
    repo = get_repo_from_dir(dir)
    repo.write_and_close do
      if recursive
        repo.outlist repo.range sub_path, true, sub_path.sub(/.$/,('\1'.ord + 1).chr), false
      else
        repo.outlist sub_path
      end
    end
  end

  def self.exists_in_repo(dir, sub_path, content)
    repo = get_repo_from_dir(dir)
    repo.read_and_close do
      repo.include? sub_path 
    end
  end

  def self.find_repo_dir(file)
    self.repository_dirs.each do |dir|
      if file.start_with? dir
        sub_path = file.to_s[dir.length..-1]
        return [dir, sub_path]
      end
    end
    nil
  end

  def self.rm(file)
    if (dir_sub_path = find_repo_dir(file))
      remove_from_repo(*dir_sub_path)
    else
      FileUtils.rm(file) if File.exists?(file)
    end
  end

  def self.rm_rf(file)
    if (dir_sub_path = find_repo_dir(file))
      remove_from_repo(dir_sub_path[0], dir_sub_path[1], true)
    else
      FileUtils.rm_rf(file)
    end
  end

  def self.file_open(file, grep, mode = 'r', invert_grep = false)
    if (dir_sub_path = find_repo_dir(file))
      stream = get_stream_from_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      stream =  File.open(file, mode)
    end

    if grep
      grep(stream, grep, invert_grep)
    else
      stream
    end
  end

  def self.file_write(file, content, mode = 'w')
    if (dir_sub_path = find_repo_dir(file))
      dir_sub_path.push content
      save_content_in_repo(*dir_sub_path)
    else
      File.open(file, mode) do |f|
        begin
          f.flock(File::LOCK_EX)
          f.write content 
          f.flock(File::LOCK_UN)
        ensure
          f.close unless f.closed?
        end
      end
    end
  end

  def self.mkdir(target)
    target = target.find if Path === target
    if not File.exists?(target)
      FileUtils.mkdir_p target
    end
  end

  def self.ln_s(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    FileUtils.mkdir_p File.dirname(target) unless File.exists?(File.dirname(target))
    FileUtils.rm target if File.exists?(target)
    FileUtils.ln_s source, target
  end

  def self.ln(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    FileUtils.mkdir_p File.dirname(target) unless File.exists?(File.dirname(target))
    FileUtils.rm target if File.exists?(target)
    FileUtils.ln source, target
  end

  def self.ln_h(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    FileUtils.mkdir_p File.dirname(target) unless File.exists?(File.dirname(target))
    FileUtils.rm target if File.exists?(target)
    begin
      CMD.cmd("ln -L '#{ source }' '#{ target }'")
    rescue ProcessFailed
      if $!.message.include? "Invalid cross-device link"
        Log.debug "Could not hard link #{source} and #{target}: cross-device link"
        CMD.cmd("cp -L '#{ source }' '#{ target }'")
      else
        raise $!
      end
    end
  end

  def self.cp(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    FileUtils.mkdir_p File.dirname(target) unless File.exists?(File.dirname(target))
    FileUtils.rm target if File.exists?(target)
    FileUtils.cp source, target
  end

  def self.mv(source, target, options = {})
    dir_sub_path_source = find_repo_dir(source)
    dir_sub_path_target = find_repo_dir(target)

    if dir_sub_path_source.nil? and dir_sub_path_target.nil?
      FileUtils.mkdir_p File.dirname(target) unless File.exist? File.dirname(target)
      tmp_target = File.join(File.dirname(target), '.tmp_mv.' + File.basename(target))
      FileUtils.mv source, tmp_target
      FileUtils.mv tmp_target, target
      return
    end

    if dir_sub_path_source.nil?
      save_content_in_repo(dir_sub_path_target[0], dir_sub_path_target[1], Open.read(source))
      return nil
    end

    if dir_sub_path_target.nil?
      Open.write(target, get_stream_from_repo(dir_sub_path_source))
      return nil
    end

    repo_source = get_repo_from_dir(dir_sub_path_source[0])
    repo_target = get_repo_from_dir(dir_sub_path_target[0])

    repo_source.write_and_close do
      repo_target.write_and_close do
        repo_source[dir_sub_path_source[1]] = repo_target[dir_sub_path_target[1]]
      end
    end

    return nil
  end

  def self.exists?(file)
    if (dir_sub_path = find_repo_dir(file))
      dir_sub_path.push file
      exists_in_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      File.exist?(file) || File.symlink?(file)
    end
  end

  def self.lock(file, options = {}, &block)
    if file and (dir_sub_path = find_repo_dir(file))
      dir, sub_path = dir_sub_path
      repo = get_repo_from_dir(dir)
      Misc.lock_in_repo(repo, sub_path, &block)
    else
      Misc.lock(file, options, &block)
    end
  end


  # Decompression
  
  def self.bgunzip(stream)
    Bgzf.setup stream
  end
   
  def self.gunzip(stream)
    CMD.cmd('zcat', :in => stream, :pipe => true, :no_fail => true, :no_wait => true)
  end

  def self.gzip(stream)
    CMD.cmd('gzip', :in => stream, :pipe => true, :no_fail => true, :no_wait => true)
  end

  def self.bgzip(stream)
    CMD.cmd('bgzip', :in => stream, :pipe => true, :no_fail => true, :no_wait => true)
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
    file = file.find if Path === file
    !! (file =~ /\.gz$/)
  end

  def self.bgzip?(file)
    file = file.find if Path === file
    !! (file =~ /\.bgz$/)
  end

  def self.zip?(file)
    file = file.find if Path === file
    !! (file =~ /\.zip$/)
  end


  # Open Read Write

  def self.clean_cache(url, options = {})
    options = Misc.add_defaults options, :noz => false, :mode => 'r'

    wget_options = options[:wget_options] || {}
    wget_options[:nice] = options.delete(:nice)
    wget_options[:nice_key] = options.delete(:nice_key)
    wget_options[:quiet] = options.delete(:quiet)
    wget_options["--post-data="] = options.delete(:post) if options.include? :post
    wget_options["--post-file"] = options.delete("--post-file") if options.include? "--post-file"
    wget_options["--post-file="] = options.delete("--post-file=") if options.include? "--post-file="
    wget_options[:cookies] = options.delete(:cookies)

    cache_file = in_cache(url, wget_options)
    Misc.lock(cache_file) do
      FileUtils.rm(cache_file)
    end if cache_file
  end

  def self.open(url, options = {})
    if IO === url
      if block_given?
        res = yield url 
        url.close
        return res
      else
        return url 
      end
    end
    options = Misc.add_defaults options, :noz => false, :mode => 'r'

    mode = Misc.process_options options, :mode

    options[:noz] = true if mode.include? "w"

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
           file_open(url, options[:grep], mode, options[:invert_grep])
         when (options[:nocache] and options[:nocache] != :update)
           # What about grep?
           wget(url, wget_options)
         when (options[:nocache] != :update and in_cache(url, wget_options))
           file_open(in_cache(url, wget_options), options[:grep], mode, options[:invert_grep])
         else
           io = wget(url, wget_options)
           add_cache(url, io, wget_options)
           file_open(in_cache(url, wget_options), options[:grep], mode, options[:invert_grep])
         end
    io = unzip(io)  if ((String === url and zip?(url))  and not options[:noz]) or options[:zip]
    io = gunzip(io) if ((String === url and gzip?(url)) and not options[:noz]) or options[:gzip]
    io = bgunzip(io) if ((String === url and bgzip?(url)) and not options[:noz]) or options[:bgzip]

    class << io;
      attr_accessor :filename
    end

    io.filename = url.to_s

    if block_given?
      begin
        return yield(io)
      rescue DontClose
      rescue Exception
        io.abort if io.respond_to? :abort
        io.join if io.respond_to? :join
        raise $!
      ensure
        io.close if io.respond_to? :close and not io.closed?
        io.join if io.respond_to? :join
      end
    end

    io
  end

  def self.download(url, file)
    Open.open(url, :mode => 'rb', :noz => true) do |sin|
      Open.open(file, :mode => 'wb') do |sout|
        Misc.consume_stream(sin, false, sout)
      end
    end
  end

  def self.can_open?(file)
    String === file and (File.exist?(file) or remote?(file))
  end

  def self.read(file, options = {}, &block)
    open(file, options) do |f|
      if block_given?
        res = []
        while not f.eof?
          l = f.gets
          l = Misc.fixutf8(l) unless options[:nofix]
          res << yield(l)
        end
        res
      else
        if options[:nofix]
          f.read
        else
          Misc.fixutf8(f.read)
        end
      end
    end
  end

  def self.notify_write(file)
    begin
      notification_file = file + '.notify'
      if File.exist? notification_file
        key = Open.read(notification_file).strip
        key = nil if key.empty?
        Misc.notify("Wrote " << file, nil, key)
        FileUtils.rm notification_file
      end
    rescue
      Log.warn "Error notifying write of #{ file }"
    end
  end

  def self.write(file, content = nil, options = {})
    options = Misc.add_defaults options, :mode => 'w'

    file = file.find(options[:where]) if Path === file
    mode = Misc.process_options options, :mode

    FileUtils.mkdir_p File.dirname(file)
    case
    when block_given?
      begin
        f = File.open(file, mode)
        begin
          yield f
        ensure
          f.close unless f.closed?
        end
      rescue Exception
        FileUtils.rm file if File.exist? file
        raise $!
      end
    when content.nil?
      File.open(file, mode){|f| f.write "" }
    when String === content
      file_write(file, content, mode)
    else
      begin
        File.open(file, mode) do |f| 
          f.flock(File::LOCK_EX)
          while block = content.read(Misc::BLOCK_SIZE)
            f.write block
          end
          f.flock(File::LOCK_UN)
        end
      rescue Exception
        FileUtils.rm_rf file if File.exist? file
        raise $!
      end
      content.close
    end
    notify_write(file) 
  end

  def self.writable?(path)
    path = path.find if Path === path
    if File.exists?(path)
      File.writable?(path)
    else
      File.writable?(File.dirname(File.expand_path(path)))
    end
  end
end
