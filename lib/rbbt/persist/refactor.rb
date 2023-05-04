module Persist
  def self.is_persisted?(path, persist_options = {})
    return true if Open.remote?(path)
    return true if Open.ssh?(path)
    return false if not Open.exists? path
    return false if TrueClass === persist_options[:update]

    expiration = persist_options[:expiration]
    if expiration
      seconds = Misc.timespan(expiration)
      patht = Open.mtime(path)
      return false if Time.now > patht + seconds
    end

    check = persist_options[:check]
    return true if check.nil?

    missing = check.reject{|file| Open.exists?(file) }
    return false if missing.any?

    return true unless ENV["RBBT_UPDATE"]

    if Array === check
      newer = check.select{|file| newer? path, file}
      return true if newer.empty?
      Log.medium "Persistence check for #{path} failed in: #{ Misc.fingerprint(newer)}"
      return false 
    else
      ! newer?(path, check)
    end
  end

  # Is 'file' newer than 'path'? return non-true if path is newer than file
  def self.newer?(path, file, by_link = false)
    return true if not Open.exists?(file)
    path = path.find if Path === path
    file = file.find if Path === file
    if by_link
      patht = File.exist?(path) ? File.lstat(path).mtime : nil
      filet = File.exist?(file) ? File.lstat(file).mtime : nil
    else
      patht = Open.mtime(path)
      filet = Open.mtime(file)
    end
    return true if patht.nil? || filet.nil?
    diff = patht - filet
    return diff if diff < 0
    return false
  end
#
end
