require 'rbbt/util/cmd'
require 'rbbt/util/tmpfile'
require 'rbbt/util/misc'
require 'rbbt/util/misc/bgzf'
require 'pathname'

require 'zlib'

module Open
  class OpenURLError < StandardError; end
  class OpenGzipError < StandardError; end

  REMOTE_CACHEDIR = File.join(ENV["HOME"], "/tmp/open_cache")
  #FileUtils.mkdir_p REMOTE_CACHEDIR unless File.exist? REMOTE_CACHEDIR

  GREP_CMD = begin
               if ENV["GREP_CMD"] 
                 ENV["GREP_CMD"]
               elsif File.exist?('/bin/grep')
                 "/bin/grep"
               elsif File.exist?('/usr/bin/grep')
                 "/usr/bin/grep"
               else
                 "grep"
               end
             end

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
      wget_options = options.dup
      wget_options = wget_options.merge( '-O' => '-') unless options.include?('--output-document')
      wget_options[:pipe] = pipe unless pipe.nil?
      wget_options[:stderr] = stderr unless stderr.nil?

      CMD.cmd("wget '#{ url }'", wget_options)
    rescue
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
  
  def self.grep(stream, grep, invert = false, fixed = nil)
    case 
    when Array === grep
      TmpFile.with_file(grep * "\n", false) do |f|
        if FalseClass === fixed
          CMD.cmd("#{GREP_CMD} #{invert ? '-v' : ''} -", "-f" => f, :in => stream, :pipe => true, :post => proc{FileUtils.rm f})
        else
          CMD.cmd("#{GREP_CMD} #{invert ? '-v' : ''} -", "-w" => true, "-F" => true, "-f" => f, :in => stream, :pipe => true, :post => proc{FileUtils.rm f})
        end
      end
    else
      CMD.cmd("#{GREP_CMD} #{invert ? '-v ' : ''} '#{grep}' -", :in => stream, :pipe => true, :post => proc{begin stream.force_close; rescue Exception; end if stream.respond_to?(:force_close)})
    end
  end

  def self.clear_dir_repos
    @@repos.clear if defined? @@repos and @@repos
  end
  def self.get_repo_from_dir(dir)
    @@repos ||= {}
    @@repos[dir] ||= begin
                      repo_path = File.join(dir, '.file_repo')
                      Persist.open_tokyocabinet(repo_path, false, :clean, TokyoCabinet::BDB )
                    end
  end

  def self.get_stream_from_repo(dir, sub_path)
    repo = get_repo_from_dir(dir)
    repo.read_and_close do
      content = repo[sub_path]
      content.nil? ? nil : StringIO.new(content).tap{|o| o.binmode }
    end
  end

  def self.get_time_from_repo(dir, sub_path)
    repo = get_repo_from_dir(dir)
    time = repo.read_and_close do
      Time.at(repo['.time.' + sub_path].to_i)
    end
    time
  end

  def self.get_atime_from_repo(dir, sub_path)
    repo = get_repo_from_dir(dir)
    File.atime(repo.persistance_path)
  end

  def self.writable_repo?(dir, sub_path)
    repo = get_repo_from_dir(dir)
    begin
      repo.write_and_close do
      end
      true
    rescue
      false
    end
  end

  def self.set_time_from_repo(dir, sub_path)
    repo = get_repo_from_dir(dir)
    repo.read_and_close do
      repo['.time.' + sub_path] = Time.now.to_i.to_s
    end
  end

  def self.save_content_in_repo(dir, sub_path, content)
    repo = get_repo_from_dir(dir)
    repo.write_and_close do
      repo['.time.' + sub_path] = Time.now.to_i.to_s
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

  def self.list_repo_files(dir, sub_path = nil)
    repo = get_repo_from_dir(dir)
    files = repo.keys
    files.reject!{|f| f[0] == "."}
    return files unless sub_path

    files.select{|file| file.start_with?(sub_path) }
  end

  def self.exists_in_repo(dir, sub_path, content)
    repo = get_repo_from_dir(dir)
    repo.read_and_close do
      repo.include?(sub_path) && ! repo[sub_path].nil?
    end
  end

  def self.find_repo_dir(file)
    self.repository_dirs.each do |dir|
      dir = dir + '/' unless dir.chars[-1] == "/"

      begin
        if file.start_with?(dir) || file == dir[0..-2]
          sub_path = file.to_s[dir.length..-1]
          return [dir, sub_path]
        else 
          if Path === file and (ffile = file.find).start_with? dir
            sub_path = ffile.to_s[dir.length..-1]
            return [dir, sub_path]
          end
        end
      end
    end
    nil
  end

  def self.rm(file)
    if (dir_sub_path = find_repo_dir(file))
      remove_from_repo(*dir_sub_path)
    else
      FileUtils.rm(file) if File.exist?(file) or Open.broken_link?(file)
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
      if mode.include? 'w'
        stream = StringIO.new
        class << stream
          attr_accessor :dir_sub_path
          def close
            self.rewind
            Open.save_content_in_repo(*dir_sub_path, self.read)
          end
        end
        stream.dir_sub_path = dir_sub_path

      else
        stream = get_stream_from_repo(*dir_sub_path)
      end
    else
      Open.mkdir File.dirname(file) if mode.include? 'w'

      file = file.find if Path === file
      stream =  File.open(file, mode)
    end

    if grep
      grep(stream, grep, invert_grep)
    else
      stream
    end
  end

  def self.ssh_open(file)
    m = file.match(/ssh:\/\/([^:]+):(.*)/)
    server = m[1]
    file = m[2]
    CMD.cmd("ssh '#{server}' cat '#{file}'", :pipe => true)
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
    if (dir_sub_path = find_repo_dir(target))
      nil
    else
      target = target.find if Path === target
      if ! File.exist?(target)
        FileUtils.mkdir_p target
      end
    end
  end

  def self.ln_s(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    target = File.join(target, File.basename(source)) if File.directory? target
    FileUtils.mkdir_p File.dirname(target) unless File.exist?(File.dirname(target))
    FileUtils.rm target if File.exist?(target)
    FileUtils.rm target if File.symlink?(target)
    FileUtils.ln_s source, target
  end

  def self.ln(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target
    source = File.realpath(source) if File.symlink?(source)

    FileUtils.mkdir_p File.dirname(target) unless File.exist?(File.dirname(target))
    FileUtils.rm target if File.exist?(target)
    FileUtils.rm target if File.symlink?(target)
    FileUtils.ln source, target
  end

  def self.ln_h(source, target, options = {})
    source = source.find if Path === source
    target = target.find if Path === target

    FileUtils.mkdir_p File.dirname(target) unless File.exist?(File.dirname(target))
    FileUtils.rm target if File.exist?(target)
    begin
      CMD.cmd("ln -L '#{ source }' '#{ target }'")
    rescue ProcessFailed
      Log.debug "Could not hard link #{source} and #{target}: #{$!.message.gsub("\n", '. ')}"
      CMD.cmd("cp -L '#{ source }' '#{ target }'")
    end
  end

  def self.link(source, target, options = {})
    begin
      Open.ln(source, target, options)
    rescue
      Open.ln_s(source, target, options)
    end
    nil
  end

  #def self.cp(source, target, options = {})
  #  source = source.find if Path === source
  #  target = target.find if Path === target

  #  FileUtils.mkdir_p File.dirname(target) unless File.exist?(File.dirname(target))
  #  FileUtils.rm target if File.exist?(target)
  #  FileUtils.cp source, target
  #end

  def self.cp(source, target, options = {})
    dir_sub_path_source = find_repo_dir(source)
    dir_sub_path_target = find_repo_dir(target)

    if dir_sub_path_source.nil? and dir_sub_path_target.nil?
      FileUtils.mkdir_p File.dirname(target) unless File.exist? File.dirname(target)
      tmp_target = File.join(File.dirname(target), '.tmp_mv.' + File.basename(target))
      FileUtils.cp_r source, tmp_target
      FileUtils.mv tmp_target, target
      return
    end

    if dir_sub_path_source.nil?
      save_content_in_repo(dir_sub_path_target[0], dir_sub_path_target[1], Open.read(source, :mode => 'rb', :nofix => true))
      return nil
    end

    if dir_sub_path_target.nil?
      Open.write(target, get_stream_from_repo(dir_sub_path_source))
      return nil
    end

    repo_source = get_repo_from_dir(dir_sub_path_source[0])
    repo_target = get_repo_from_dir(dir_sub_path_target[0])

    content = repo_source.read_and_close do
      repo_source[dir_sub_path_source[1]]
    end

    repo_target.write_and_close do
      repo_target[dir_sub_path_target[1]] = content
    end

    return nil
  end

  def self.mv(source, target, options = {})
    dir_sub_path_source = find_repo_dir(source)
    dir_sub_path_target = find_repo_dir(target)

    if dir_sub_path_source.nil? and dir_sub_path_target.nil?
      FileUtils.mkdir_p File.dirname(target) unless File.exist? File.dirname(target)
      tmp_target = File.join(File.dirname(target), '.tmp_mv.' + File.basename(target))
      FileUtils.mv source, tmp_target
      FileUtils.mv tmp_target, target
      return nil
    end

    if dir_sub_path_source.nil?
      save_content_in_repo(dir_sub_path_target[0], dir_sub_path_target[1], Open.read(source, :mode => 'rb', :nofix => true))
      return nil
    end

    if dir_sub_path_target.nil?
      Open.write(target, get_stream_from_repo(dir_sub_path_source))
      return nil
    end

    repo_source = get_repo_from_dir(dir_sub_path_source[0])
    repo_target = get_repo_from_dir(dir_sub_path_target[0])

    content = repo_source.read_and_close do
      repo_source[dir_sub_path_target[1]]
    end

    repo_target.write_and_close do
      repo_target[dir_sub_path_source[1]] = content
    end

    repo_source.write_and_close do
      repo_source.delete dir_sub_path_source[1]
    end

    return nil
  end

  def self.exists?(file)
    if (dir_sub_path = find_repo_dir(file))
      dir_sub_path.push file
      exists_in_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      File.exist?(file) #|| File.symlink?(file)
    end
  end

  class << self
    alias exist? exists?
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
    !! (file =~ /^(?:https?|ftp|ssh):\/\//)
  end

  def self.ssh?(file)
    !! (file =~ /^ssh:\/\//)
  end

  def self.gzip?(file)
    file = file.find if Path === file
    file = file.filename if File === file
    return false unless String === file
    !! (file =~ /\.gz$/)
  end

  def self.bgzip?(file)
    file = file.find if Path === file
    file = file.filename if File === file
    return false unless String === file
    !! (file =~ /\.bgz$/)
  end

  def self.zip?(file)
    file = file.find if Path === file
    file = file.filename if File === file
    return false unless String === file
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
         when (not remote?(url) and not ssh?(url))
           file_open(url, options[:grep], mode, options[:invert_grep])
         when (options[:nocache] and options[:nocache] != :update)
           # What about grep?
           if ssh?(url)
             ssh_open(url)
           else
             wget(url, wget_options)
           end
         when (options[:nocache] != :update and in_cache(url, wget_options))
           file_open(in_cache(url, wget_options), options[:grep], mode, options[:invert_grep])
         else
           io = if ssh?(url)
                  ssh_open(url)
                else
                  wget(url, wget_options)
                end
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

  def self.download_old(url, file)
    Open.open(url, :mode => 'rb', :noz => true) do |sin|
      Open.open(file, :mode => 'wb') do |sout|
        Misc.consume_stream(sin, false, sout)
      end
    end
  end

  def self.download(url, path)
    begin
      Open.wget(url, "--output-document" => path, :pipe => false)
    rescue Exception
      Open.rm(path) if Open.exist?(path)
      raise $!
    end
  end

  def self.can_open?(file)
    String === file and (Open.exist?(file) or remote?(file))
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
      if Open.exists? notification_file
        key = Open.read(notification_file).strip
        key = nil if key.empty?
        if key && key.include?("@")
          to = from = key
          subject = "Wrote " << file
          message = "Content attached"
          Misc.send_email(from, to, subject, message, :files => [file])
        else
          Misc.notify("Wrote " << file, nil, key)
        end
        Open.rm notification_file
      end
    rescue
      Log.exception $!
      Log.warn "Error notifying write of #{ file }"
    end
  end

  def self.write(file, content = nil, options = {})
    options = Misc.add_defaults options, :mode => 'w'

    file = file.find(options[:where]) if Path === file
    mode = Misc.process_options options, :mode

    if (dir_sub_path = find_repo_dir(file))
      content = case content
                when String
                  content
                when nil 
                  if block_given?
                    yield
                  else
                    ""
                  end
                else
                  content.read
                end
      dir_sub_path.push content
      save_content_in_repo(*dir_sub_path)
    else
      FileUtils.mkdir_p File.dirname(file) unless File.directory?(file)
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
        content.join if content.respond_to? :join
      end
    end

    notify_write(file) 
  end

  def self.writable?(path)
    path = path.find if Path === path
    if (dir_sub_path = find_repo_dir(path))
      writable_repo?(*dir_sub_path)
    else
      if File.symlink?(path)
        File.writable?(File.dirname(path))
      elsif File.exist?(path)
        File.writable?(path)
      else
        File.writable?(File.dirname(File.expand_path(path)))
      end
    end
  end

  def self.ctime(file)
    if (dir_sub_path = find_repo_dir(file))
      get_time_from_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      File.ctime(file)
    end
  end

  def self.realpath(file)
    file = file.find if Path === file
    Pathname.new(File.expand_path(file)).realpath.to_s 
  end

  def self.mtime(file)
    if (dir_sub_path = find_repo_dir(file))
      get_time_from_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      begin
        if File.symlink?(file) || File.stat(file).nlink > 1
          if File.exist?(file + '.info') && defined?(Step)
            done = Step::INFO_SERIALIZER.load(Open.open(file + '.info'))[:done]
            return done if done
          end

          file = Pathname.new(file).realpath.to_s 
        end
        return nil unless File.exist?(file)
        File.mtime(file)
      rescue
        nil
      end
    end
  end

  def self.update_mtime(path, target)
    if File.symlink?(target) || File.stat(target).nlink > 1
      if File.exist?(target + '.info')
        target = target + '.info'
      else
        target = Pathname.new(target).realpath.to_s 
      end
    end

    CMD.cmd("touch -r '#{path}' '#{target}'")
    CMD.cmd("touch -r '#{path}.info' '#{target}'") if File.exist?(path + '.info')
  end

  def self.atime(file)
    if (dir_sub_path = find_repo_dir(file))
      get_atime_from_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      File.atime(file)
    end
  end

  def self.touch(file)
    if (dir_sub_path = find_repo_dir(file))
      set_time_from_repo(*dir_sub_path)
    else
      file = file.find if Path === file
      FileUtils.touch(file)
    end
  end

  def self.broken_link?(path)
    File.symlink?(path) && ! File.exist?(File.readlink(path))
  end

end
