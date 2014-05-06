Lockfile.refresh = false if ENV["RBBT_NO_LOCKFILE_REFRESH"] == "true"

module Misc

  LOCK_MUTEX = Mutex.new
  def self.lock(file, unlock = true, options = {})
    return yield if file.nil?
    FileUtils.mkdir_p File.dirname(File.expand_path(file)) unless File.exists?  File.dirname(File.expand_path(file))

    res = nil

    lock_path = File.expand_path(file + '.lock')
    lockfile = Lockfile.new(lock_path, options)

    hostname = Misc.hostname
    LOCK_MUTEX.synchronize do
      Misc.insist 2, 0.1 do
        Misc.insist 3, 0.1 do
          begin
            if File.exists? lock_path
              info = Open.open(lock_path){|f| YAML.load(f) }
              raise "No info" unless info

              if hostname == info["host"] and not Misc.pid_exists?(info["pid"])
                Log.high("Removing lockfile: #{lock_path}. This pid #{Process.pid}. Content: #{info.inspect}")
                FileUtils.rm lock_path
              end
            end
          rescue Exception
            FileUtils.rm lock_path if File.exists? lock_path
            lockfile = Lockfile.new(lock_path, options) unless File.exists? lock_path
            raise $!
          end
        end
      end
      lockfile.lock 
    end

    begin
      res = yield lockfile
    rescue Lockfile::StolenLockError
      unlock = false
    rescue KeepLocked
      unlock = false
      res = $!.payload
    rescue Exception
      lockfile.unlock if lockfile.locked?
      raise $!
    ensure
      if unlock 
        lockfile.unlock if lockfile.locked?
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
