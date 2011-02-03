require 'rbbt/util/tsv'
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
    name = prefix.to_s << ":" << file.to_s << ":"
    File.join(CACHEDIR, name.to_s.gsub(/\s/,'_').gsub(/\//,'>') + Digest::MD5.hexdigest([file, options].inspect))
  end

  def self.get_filename(file)
    case
    when (String === file and File.exists? file)
      File.expand_path file
    when File === file
      File.expand_path file.path
    when Object::TSV === file
      file.filename
    when String === file
      file
    else
      file.class.to_s
    end
  end

  def self.persist_string(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"

      res = yield file, options, filename, persistence_file
      Open.write(persistence_file, res.to_s)
      res
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"

      Open.read(persistence_file)
    end
  end

  def self.persist_marshal(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"
      res = yield file, options
      Open.write(persistence_file, Marshal.dump(res))
      res
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"
      Marshal.load(Open.open(persistence_file))
    end
  end

  def self.persist_yaml(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"
      res = yield file, options, filename, persistence_file
      Open.write(persistence_file, YAML.dump(res))
      res
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"
      YAML.load(Open.open(persistence_file))
    end
  end

  def self.persist_tsv_string(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"
      res = yield file, options, filename, persistence_file
      Open.write(persistence_file, res.to_s)
      res
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"
      Object::TSV.new persistence_file, :filename => filename
    end
  end

  def self.tsv_serializer(data)
    case
    when (not Object::TSV === data)
      :marshal
    when data.type == :double
      :double
    when data.type == :single
      :single
    else
      :list
    end
  end

  def self.persist_tsv(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"

      res = yield file, options, filename, persistence_file
      serializer = tsv_serializer res

      if File.exists? persistence_file
        Log.debug "Erasing old #{ persistence_file }. Prefix = #{prefix}"
        FileUtils.rm persistence_file
      end

      per = Persistence::TSV.get persistence_file, true, serializer

      per.write
      per.merge! res
      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if res.respond_to?(key.to_sym)  and per.respond_to?(key.to_sym)
          per.send "#{key}=".to_sym, res.send(key.to_sym) 
        end
      end
      per.read

      tsv = Object::TSV.new per
      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if tsv.respond_to?(key.to_sym)  and per.respond_to?(key.to_sym)
          tsv.send "#{key}=".to_sym, per.send(key.to_sym) 
        end
      end

      tsv
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"

      per = Persistence::TSV.get persistence_file, true, serializer
      tsv = Object::TSV.new per
      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if tsv.respond_to?(key.to_sym)  and per.respond_to?(key.to_sym)
          tsv.send "#{key}=".to_sym, per.send(key.to_sym) 
        end
      end
      tsv
    end
  end

  def self.persist_tsv_extra(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating #{ persistence_file }. Prefix = #{prefix}"
      res, extra = yield file, options, filename, persistence_file
      serializer = tsv_serializer res

      per = Persistence::TSV.get persistence_file, true, serializer

      per.write
      per.merge! res
      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if extra.include?(key.to_sym)  and per.respond_to?(key.to_sym)
          per.send "#{key}=".to_sym, extra[key.to_sym]
        end
      end
      per.read

     [ per, extra ]
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"
      per = Persistence::TSV.get persistence_file, true, serializer

      extra = {}
      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if per.respond_to?(key.to_sym)
          extra[key] = per.send(key.to_sym)
        end
      end

     [ per, extra ]
    end
  end

  def self.persist(file, prefix = "", persistence_type = :string, options = {}, &block)
    options = Misc.add_defaults options, :persistence => true
    persistence =
      Misc.process_options options, :persistence

    filename = get_filename(file)

    if not persistence
      Log.low "Non Persistent Loading for #{filename}. Prefix: #{prefix}"
      yield file, options
    else
      Log.low "Persistent Loading for #{filename}. Prefix: #{prefix}. Type #{persistence_type.to_s}"

      case persistence_type.to_sym
      when :string
        persist_string(file, prefix, options, &block)
      when :marshal
        persist_marshal(file, prefix, options, &block)
      when :yaml
        persist_yaml(file, prefix, options, &block)
      when :tsv
        persist_tsv(file, prefix, options, &block)
      when :tsv_string
        persist_tsv_string(file, prefix, options, &block)
      when :tsv_extra
        persist_tsv_extra(file, prefix, options, &block)
      end
    end
  end


  def self.persist2(file, prefix = "", persistence_type = :string, options = {})
    options = Misc.add_defaults options, :persistence => false, :persistence_file => nil, :persistence_update => false, :tsv_serializer => :marshal, :force_array => false

    persistence, persistence_file, persistece_update, tsv_serializer, force_array =
      Misc.process_options options, :persistence, :persistence_file, :persistence_update, :tsv_serializer, :force_array

    persistence = true if not persistence_file.nil?

    filename = Misc.process_options options, :persistence_source
    filename ||= case
                 when (String === file and File.exists? file)
                   File.expand_path file
                 when File === file
                   File.expand_path file.path
                 when Object::TSV === file
                   file.filename
                 when String === file
                   file
                 else
                   file.class.to_s
                 end

    if persistence
      persistence_file ||= get_persistence_file(filename, prefix, options)

      #{{{ CREATE
      if persistece_update or not File.exists? persistence_file
        Log.low "Creating Persistent #{prefix} for #{ filename }"

        res = yield file, options, filename, persistence_file

        if Array === res and res.length == 2 and (Hash === res[1] or res[1].nil?)
          data, extra = res
        else
          data, extra = [res, nil]
        end

        case persistence_type.to_sym
        when :tsv 
          if Hash === data or Object::TSV === data 
            tsv_serializer ||= case
                               when (not Object::TSV === data)
                                 :marshal
                               when data.type == :double
                                 :double
                               when data.type == :single
                                 :single
                               else
                                 :list
                               end

            Log.debug "Creating [#{tsv_serializer}] #{Persistence::TSV} in #{ persistence_file }"


            per = Persistence::TSV.get persistence_file, true, tsv_serializer
            per.write
            per.merge! data

            Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
              if data.respond_to?(key.to_sym)  and per.respond_to?(key.to_sym)
                per.send "#{key}=".to_sym, data.send(key.to_sym) 
              else
                per.send "#{key}=".to_sym, extra[key.to_sym] if extra and extra.include? key.to_sym
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

        if Array === res and res.length == 2 and (Hash === res[1] or res[1].nil?)
          if persistence_type.to_sym == :tsv and not force_array
            Object::TSV.new [data, extra]
          else
            return [data, extra]
          end
        else
          return data
        end

        #{{{ LOAD
      else
        Log.low "Opening Persistent #{prefix} for #{ filename }"
        case persistence_type.to_sym
        when :tsv
          data        = Persistence::TSV.get persistence_file

          extra = {}
          Persistence::TSV::FIELD_INFO_ENTRIES.keys.each{|key| extra[key.to_sym] = data.send key.to_sym}

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
      Log.low "Non Persistent #{prefix} for #{ filename }"
      yield file, options
    end
  end
end
