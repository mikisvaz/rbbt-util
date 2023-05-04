require 'rbbt/persist/tsv/adapter'

require 'rbbt/persist/tsv/fix_width_table'
require 'rbbt/persist/tsv/packed_index'

begin
  require 'rbbt/persist/tsv/tokyocabinet'
rescue Exception
  Log.warn "The tokyocabinet gem could not be loaded. Persistence using this engine will fail."
end

begin
  require 'rbbt/persist/tsv/lmdb'
rescue Exception
  Log.debug "The lmdb gem could not be loaded. Persistence using this engine will fail."
end

begin
  require 'rbbt/persist/tsv/leveldb'
rescue Exception
  Log.debug "The LevelDB gem could not be loaded. Persistence using this engine will fail."
end

begin
  require 'rbbt/persist/tsv/cdb'
rescue Exception
  Log.debug "The CDB gem could not be loaded. Persistence using this engine will fail."
end

begin
  require 'rbbt/persist/tsv/kyotocabinet'
rescue Exception
  Log.debug "The kyotocabinet gem could not be loaded. Persistence using this engine will fail."
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

  def self.open_database(path, write, serializer = nil, type = "HDB", options = {})
    case type
    when "LevelDB"
      Persist.open_leveldb(path, write, serializer)
    when "CDB"
      Persist.open_cdb(path, write, serializer)
    when "LMDB"
      Persist.open_lmdb(path, write, serializer)
    when 'kch', 'kct'
      Persist.open_kyotocabinet(path, write, serializer, type)
    when 'fwt'
      value_size, range, update, in_memory, pos_function = Misc.process_options options.dup, :value_size, :range, :update, :in_memory, :pos_function
      if pos_function
        Persist.open_fwt(path, value_size, range, serializer, update, in_memory, &pos_function)
      else
        Persist.open_fwt(path, value_size, range, serializer, update, in_memory)
      end
    when 'pki'
      pattern, pos_function = Misc.process_options options.dup, :pattern, :pos_function
      if pos_function
        Persist.open_pki(path, write, pattern, &pos_function)
      else
        Persist.open_pki(path, write, pattern)
      end
    else
      Persist.open_tokyocabinet(path, write, serializer, type)
    end
  end

  def self.persist_tsv(source, filename = nil, options = {}, persist_options = {}, &block)
    persist_options[:prefix] ||= "TSV"

    if data = persist_options[:data]
      Log.debug "TSV persistence creating with data: #{ Misc.fingerprint(data) }"
      yield data
      return data 
    end

    filename ||= get_filename(source)

    if not persist_options[:persist]
      data = {}

      yield(data) 

      return data 
    end

    path = persistence_path(filename, options)

    if ENV["RBBT_UPDATE_TSV_PERSIST"] == 'true' and filename
      check_options = {:check => [filename]}
    else
      check_options = {}
    end

    if is_persisted?(path, check_options) and not persist_options[:update]
      path = path.find if Path === path
      Log.debug "TSV persistence up-to-date: #{ path }"
      if persist_options[:shard_function]
        return open_sharder(path, false, nil, persist_options[:engine], persist_options, &persist_options[:shard_function]) 
      else
        return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
      end
    end

    lock_filename = Persist.persistence_path(path, {:dir => TSV.lock_dir})
    Misc.lock lock_filename do
      begin
        if is_persisted?(path, check_options) and not persist_options[:update]
          path = path.find if Path === path
          Log.debug "TSV persistence (suddenly) up-to-date: #{ path }"

          if persist_options[:shard_function]
            return open_sharder(path, false, nil, persist_options[:engine], persist_options, &persist_options[:shard_function]) 
          else
            return open_database(path, false, nil, persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
          end
        end
        path = path.find if Path === path

        FileUtils.rm_rf path if File.exist? path

        Log.medium "TSV persistence creating: #{ path }"

        tmp_path = path + '.persist'

        data = if persist_options[:shard_function]
                 open_sharder(tmp_path, true, persist_options[:serializer], persist_options[:engine], persist_options, &persist_options[:shard_function]) 
               else
                 open_database(tmp_path, true, persist_options[:serializer], persist_options[:engine] || TokyoCabinet::HDB, persist_options) 
               end

        if TSV === data and data.serializer.nil?
          data.serializer = :type 
        end

        if persist_options[:persist] == :preload
          tmp_tsv = yield({})
          tmp_tsv.annotate data
          data.serializer = tmp_tsv.type
          data.write_and_read do
            tmp_tsv.each do |k,v|
              data[k] = v
            end
          end
        else
          data.write_and_read do
            yield data
          end
        end

        data.write_and_read do
          FileUtils.mv data.persistence_path, path if File.exist? data.persistence_path and not File.exist? path
          tsv = CONNECTIONS[path] = CONNECTIONS.delete tmp_path
          tsv.persistence_path = path

          tsv.fix_io if tsv.respond_to? :fix_io
        end

        data
      rescue Exception
        Log.error "Captured error during persist_tsv. Erasing: #{path}"
        FileUtils.rm_rf tmp_path if tmp_path and File.exist? tmp_path
        FileUtils.rm_rf path if path and File.exist? path
        raise $!
      end
    end
  end

end

require 'rbbt/persist/tsv/sharder'

