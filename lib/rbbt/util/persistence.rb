require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'yaml'

module Persistence
  require 'rbbt/util/tc_hash'
  TSV = TCHash

  CACHEDIR="/tmp/tsv_persistent_cache"
  FileUtils.mkdir CACHEDIR unless File.exist? CACHEDIR

  def self.cachedir=(cachedir)
    CACHEDIR.replace cachedir
    FileUtils.mkdir_p CACHEDIR unless File.exist? CACHEDIR
  end

  def self.cachedir
    CACHEDIR
  end
 
  def self.get_persistence_file(file, prefix, options = {})
    File.join(CACHEDIR, prefix.to_s.gsub(/\s/,'_').gsub(/\//,'>') + Digest::MD5.hexdigest([file, options].inspect))
  end

  def self.persist(file, prefix = "", persistence_type = :string, options = {})
    options = Misc.add_defaults options, :persistence => true

    persistence, persistence_file =
      Misc.process_options options, :persistence, :persistence_file

    filename = Misc.process_options options, :filename
    filename ||= case
                  when (String === file and File.exists? file)
                    File.expand_path file
                  when File === file
                    File.expand_path file.path
                  when TSV === file
                    file.filename
                  else
                    Digest::MD5.hexdigest(file.inspect)
                  end

    if persistence
      persistence_file ||= get_persistence_file(filename, prefix, options)
      
      #{{{ CREATE
      if ! File.exists? persistence_file
        Log.low "Creating Persistence #{ persistence_file } for #{ filename }"
        res = yield file, options, filename, persistence_file
        if Array === res and res.length == 2 and (Hash === res[1] or res[1].nil?)
          data, extra = res
        else
          data, extra = [res, nil]
        end

        case persistence_type.to_sym
        when :tsv 
          if Hash === data or Object::TSV === data or Persistence::TSV === data
            Log.debug "Creating #{Persistence::TSV} for #{ persistence_file }"
            per = Persistence::TSV.get persistence_file
            per.write
            data.each{|k,v| per[k.to_s] = v}
            %w(case_insensitive fields key_field type filename).each do |key| 
              if data.respond_to? key
                per.send "#{key}=".to_sym, data.send(key.to_sym) 
              else
                per.send "#{key}=".to_sym, extra[key.to_sym]
              end
            end
            per.read

            data = per
          end
        when :string
          Open.write(persistence_file, data.to_s)
        when :marshal
          Open.write(persistence_file, Marshal.dump(data))
        when :yaml
          Open.write(persistence_file, YAML.dump(data))
        end
        
        return [data, extra]

      #{{{ LOAD
      else
        Log.low "Opening Persistence #{ persistence_file } for #{ filename }"
        case persistence_type.to_sym
        when :tsv
          data        = Persistence::TSV.get persistence_file
          
          extra = {}
          %W(case_insensitive fields key_field type filename).each{|key| extra[key.to_sym] = data.send key.to_sym}

          return [data, extra]
        when :string
          return [Open.read(persistence_type), nil]
        when :marshal
          return [File.open(persistence_file){|f| Marshal.load(f)}, nil]
        when :yaml
          return [File.open(persistence_file){|f| YAML.load(f)}, nil]
        end

      end
    else
      yield file, options
    end
  end
end
