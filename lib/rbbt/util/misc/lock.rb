module Misc
  def self.use_lock_id=(use = true)
    if use
      Log.medium "Activating lockfile ids"
      Lockfile.dont_use_lock_id = false
      Lockfile.refresh = 2 
      Lockfile.max_age = 30
      Lockfile.suspend = 4
    else
      Log.medium "De-activating lockfile ids"
      Lockfile.dont_use_lock_id = true
      Lockfile.refresh = 4
      Lockfile.max_age = 60
      Lockfile.suspend = 8
    end
  end

  self.use_lock_id = ENV["RBBT_NO_LOCKFILE_ID"] != "true"

  LOCK_MUTEX = Mutex.new
  def self.lock(file, unlock = true, options = {})
    unlock, options = true, unlock if Hash === unlock
    return yield if file.nil? and not Lockfile === options[:lock]

    file = file.find if Path === file
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exist? File.dirname(File.expand_path(file))


    case options[:lock]
    when Lockfile
      lockfile = options[:lock]
      lockfile.lock unless lockfile.locked?
    when FalseClass
      lockfile = nil
      unlock = false
    when Path, String
      lock_path = options[:lock].find
      lockfile = Lockfile.new(lock_path, options)
      lockfile.lock 
    else
      lock_path = File.expand_path(file + '.lock')
      lockfile = Lockfile.new(lock_path, options)
      lockfile.lock 
    end

    res = nil

    begin
      res = yield lockfile
    rescue KeepLocked
      unlock = false
      res = $!.payload
    ensure
      if unlock 
        begin
          if lockfile.locked?
            lockfile.unlock 
          else
          end
        rescue Exception
          Log.warn "Exception unlocking: #{lockfile.path}"
          Log.exception $!
        end
      end
    end

    res
  end
  

  LOCK_REPO_SERIALIZER=Marshal
  def self.lock_in_repo(repo, key, *args)
    return yield file, *args if repo.nil? or key.nil?

    lock_key = "lock-" << key

    begin
      if repo[lock_key] and
        Misc.hostname == (info = LOCK_REPO_SERIALIZER.load(repo[lock_key]))["host"] and 
        info["pid"] and not Misc.pid_exists?(info["pid"])

        Log.info("Removing lockfile: #{lock_key}. This pid #{Process.pid}. Content: #{info.inspect}")
        repo.out lock_key 
      end
    rescue
      Log.warn("Error checking lockfile #{lock_key}: #{$!.message}. Removing. Content: #{begin repo[lock_key] rescue "Could not open file" end}")
      repo.out lock_key if repo.include? lock_key
    end

    while repo[lock_key]
      sleep 1
    end
    
    repo[lock_key] = LOCK_REPO_SERIALIZER.dump({:hostname => Misc.hostname, :pid => Process.pid})

    res = yield lock_key, *args

    repo.delete lock_key

    res
  end
end
