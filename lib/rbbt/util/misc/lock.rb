module Misc
  def self.use_lock_id=(use = true)
    if use
      Log.medium "Activating lockfile ids"
      Lockfile.dont_use_lock_id = false
      #Lockfile.refresh = 20 
      #Lockfile.max_age = 60
      #Lockfile.suspend = 2
    else
      Log.medium "De-activating lockfile ids"
      Lockfile.dont_use_lock_id = true
      #Lockfile.refresh = 5
      #Lockfile.max_age = 60 * 10
      #Lockfile.suspend = 5
    end

    Lockfile.refresh = 10
    Lockfile.max_age = 60
    Lockfile.suspend = 2
  end

  self.use_lock_id = ENV["RBBT_NO_LOCKFILE_ID"] != "true"

  LOCK_MUTEX = Mutex.new
  def self.lock(file, unlock = true, options = {})
    unlock, options = true, unlock if Hash === unlock
    return yield if file.nil?
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exists?  File.dirname(File.expand_path(file))

    res = nil

    case options[:lock]
    when Lockfile
      lockfile = options[:lock]
      lockfile.lock unless lockfile.locked?
    when FalseClass
      lockfile = nil
      unlock = false
    when Path, String
      lock_path = options[:lock]
      lockfile = Lockfile.new(lock_path, options)
      lockfile.lock 
    else
      lock_path = File.expand_path(file + '.lock')
      lockfile = Lockfile.new(lock_path, options)
      lockfile.lock 
    end

    begin
      res = yield lockfile
    rescue KeepLocked
      unlock = false
      res = $!.payload
    ensure
      if unlock 
        begin
          lockfile.unlock #if lockfile.locked?
        rescue Exception
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
