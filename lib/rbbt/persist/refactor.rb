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

  def self.persist_tsv(source, filename = nil, options = {}, persist_options = {}, &block)
    engine = IndiferentHash.process_options persist_options, :engine, engine: "HDB"
    Persist.persist(name, engine, persist_options, &block)
  end

#
#  def self.persist_tsv(source, filename = nil, options = {}, persist_options = {}, &block)
#    persist_options[:prefix] ||= "TSV"
#
#    if data = persist_options[:data]
#      Log.debug "TSV persistence creating with data: #{ Misc.fingerprint(data) }"
#      yield data
#      return data 
#    end
#
#    filename ||= get_filename(source)
#
#    if not persist_options[:persist]
#      data = {}
#
#      yield(data) 
#
#      return data 
#    end
#
#    path = persistence_path(filename, options)
#
#    if ENV["RBBT_UPDATE_TSV_PERSIST"] == 'true' and filename
#      check_options = {:check => [filename]}
#    else
#      check_options = {}
#    end
#
#    if is_persisted?(path, check_options) and not persist_options[:update]
#      path = path.find if Path === path
#      Log.debug "TSV persistence up-to-date: #{ path }"
#      if persist_options[:shard_function]
#        return open_sharder(path, false, nil, persist_options[:engine], persist_options, &persist_options[:shard_function]) 
#      else
#        return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
#      end
#    end
#
#    lock_filename = Persist.persistence_path(path, {:dir => TSV.lock_dir})
#    Misc.lock lock_filename do
#      begin
#        if is_persisted?(path, check_options) and not persist_options[:update]
#          path = path.find if Path === path
#          Log.debug "TSV persistence (suddenly) up-to-date: #{ path }"
#
#          if persist_options[:shard_function]
#            return open_sharder(path, false, nil, persist_options[:engine], persist_options, &persist_options[:shard_function]) 
#          else
#            return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
#          end
#        end
#        path = path.find if Path === path
#
#        FileUtils.rm_rf path if File.exist? path
#
#        Log.medium "TSV persistence creating: #{ path }"
#
#        tmp_path = path + '.persist'
#
#        data = if persist_options[:shard_function]
#                 open_sharder(tmp_path, true, persist_options[:serializer], persist_options[:engine], persist_options, &persist_options[:shard_function]) 
#               else
#                 open_database(tmp_path, true, persist_options[:serializer], persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
#               end
#
#        if TSV === data and data.serializer.nil?
#          data.serializer = :type 
#        end
#
#        if persist_options[:persist] == :preload
#          tmp_tsv = yield({})
#          tmp_tsv.annotate data
#          data.serializer = tmp_tsv.type
#          data.write_and_read do
#            tmp_tsv.each do |k,v|
#              data[k] = v
#            end
#          end
#        else
#          data.write_and_read do
#            yield data
#          end
#        end
#
#        data.write_and_read do
#          FileUtils.mv data.persistence_path, path if File.exist? data.persistence_path and not File.exist? path
#          tsv = CONNECTIONS[path] = CONNECTIONS.delete tmp_path
#          tsv.persistence_path = path
#
#          tsv.fix_io if tsv.respond_to? :fix_io
#        end
#
#        data
#      rescue Exception
#        Log.error "Captured error during persist_tsv. Erasing: #{path}"
#        FileUtils.rm_rf tmp_path if tmp_path and File.exist? tmp_path
#        FileUtils.rm_rf path if path and File.exist? path
#        raise $!
#      end
#    end
#  end
end

Persist.save_drivers[:annotations] = proc do |file,content|
  Persist.save(content, file, :meta_extension)
end

Persist.load_drivers[:annotations] = proc do |file|
  Persist.load(file, :meta_extension)
end
