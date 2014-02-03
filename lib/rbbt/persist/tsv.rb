require 'rbbt/persist/tsv/lmdb'

begin
  require 'rbbt/persist/tsv/tokyocabinet'
rescue Exception
  Log.warn "The tokyocabinet gem could not be loaded: persistence over TSV files will fail"
end

begin
  require 'rbbt/persist/tsv/kyotocabinet'
rescue Exception
  Log.warn "The kyotocabinet gem could not be loaded: persistence over TSV files will fail"
end

module Persist
  CONNECTIONS = {}

  def self.get_filename(source)
    case
    when Path === source
      source
    when (source.respond_to?(:filename) and source.filename)
      source.filename
    when source.respond_to?(:cmd)
      "CMD-#{Misc.digest(source.cmd)}"
    when TSV === source
      "TSV[#{Misc.digest Misc.fingerprint(source)}]"
    end || source.object_id.to_s
  end

  def self.open_database(path, write, serializer = nil, type = "HDB")
    case type
    when "LMDB"
      Persist.open_lmdb(path, write, serializer)
    when 'kch'
      Persist.open_kyotocabinet(path, write, serializer, type)
    else
      Persist.open_tokyocabinet(path, write, serializer, type)
    end
  end

  def self.persist_tsv(source, filename, options = {}, persist_options = {}, &block)
    persist_options[:prefix] ||= "TSV"

    if data = persist_options[:data]
      yield data
      return data 
    end

    filename ||= get_filename(source)

    if not persist_options[:persist]
      data = {}

      yield(data) 

      return data 
    end

    path = persistence_path(filename, persist_options, options)

    lock_filename = Persist.persistence_path(path, {:dir => TSV.lock_dir})

    if is_persisted? path and not persist_options[:update]
      Log.debug "TSV persistence up-to-date: #{ path }"
      return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB) 
    end

    Misc.lock lock_filename do
      begin
        if is_persisted? path 
          Log.debug "TSV persistence up-to-date: #{ path }"
          return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB) 
        end

        FileUtils.rm path if File.exists? path

        Log.medium "TSV persistence creating: #{ path }"

        tmp_path = path + '.persist'

        data = open_database(tmp_path, true, persist_options[:serializer], persist_options[:engine] || TokyoCabinet::HDB)
        data.serializer = :type if TSV === data and data.serializer.nil?

        data.write_and_read do
          yield data
        end

        FileUtils.mv tmp_path, path if File.exists? tmp_path and not File.exists? path
        tsv = CONNECTIONS[path] = CONNECTIONS.delete tmp_path
        tsv.persistence_path = path

        data
      rescue Exception
        Log.error "Captured error during persist_tsv. Erasing: #{path}"
        FileUtils.rm tmp_path if tmp_path and File.exists? tmp_path
        FileUtils.rm path if path and File.exists? path
        raise $!
      end
    end
  end
end
