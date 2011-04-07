require 'rbbt/util/tsv'
require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'digest/md5'
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
    persistence_dir = Misc.process_options options, :persistence_dir
    persistence_dir ||= CACHEDIR
    name = prefix.to_s << ":" << file.to_s << ":"

    options_md5 = Misc.hash2md5 options
    File.join(persistence_dir, name.to_s.gsub(/\s/,'_').gsub(/\//,'>') + options_md5)
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
      tsv = Object::TSV.new persistence_file
      tsv.filename = filename
      tsv
    end
  end

  def self.tsv_serializer(data, extra = nil)
    if Object::TSV === data
      return :integer if (data.cast == "to_i" or data.cast == :to_i) and data.type == :single
      return :integer_array if (data.cast == "to_i" or data.cast == :to_i) and (data.type == :list or data.type == :flat)

      case
      when data.type == :double
        :double
      when data.type == :list
        :list
      when data.type == :single
        :single
      else
        :marshal
      end
    else
      return :marshal if extra.nil?
      return :integer if (extra[:cast] == "to_i" or extra[:cast] == :to_i) and extra[:type] == :single
      return :integer_array if (extra[:cast] == "to_i" or extra[:cast] == :to_i) and (extra[:type] == :list or extra[:type] == :flat)

      case
      when extra[:type] == :double
        :double
      when extra[:type] == :list
        :list
      when extra[:type] == :single
        :single
      else
        :marshal
      end
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

      Log.debug "Dump data into '#{persistence_file}'"
      per = Persistence::TSV.get persistence_file, true, serializer

      per.write
      per.merge! res

      Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
        if res.respond_to?(key.to_sym)  and per.respond_to?("#{key}=".to_sym)
          per.send "#{key}=".to_sym, res.send(key.to_sym) 
        end
      end

      tsv = Object::TSV.new per

      per.read

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
      
      per.read

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

      serializer = tsv_serializer res, extra

      per = nil
      if not Persistence::TSV === res
        begin
          per = Persistence::TSV.get persistence_file, true, serializer

          per.write
          per.merge! res
          Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
            if extra.include?(key.to_sym)  and per.respond_to?(key.to_sym)
              per.send "#{key}=".to_sym, extra[key.to_sym]
            end
          end

        rescue Exception
          per.close
          raise $!
        end
      else
        per = res
      end

     [ per, extra ]
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"
      begin
        per = Persistence::TSV.get persistence_file, false, serializer

        extra = {}
        Persistence::TSV::FIELD_INFO_ENTRIES.keys.each do |key| 
          if per.respond_to?(key.to_sym)
            extra[key] = per.send(key.to_sym)
          end
        end

      rescue Interrupt
        raise "Interrupted"
      rescue Exception
        per.close
        raise $!
      end

      [ per, extra ]
    end
  end

  def self.persist_fwt(file, prefix = "", options = {})
    options = 
      Misc.add_defaults options, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_file, filename =
      Misc.process_options options, :persistence_update, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, options)

    if persistence_update or not File.exists? persistence_file
      Log.debug "Creating FWT #{ persistence_file }. Prefix = #{prefix}"

      range = options[:range]

      res = yield file, options, filename, persistence_file

      if File.exists? persistence_file
        Log.debug "Erasing old #{ persistence_file }. Prefix = #{prefix}"
        FileUtils.rm persistence_file
      end

      max_length = res.collect{|k,v| k.length}.max

      if range
        begin
          fwt = FixWidthTable.new persistence_file, max_length, true
          fwt.add_range res
        rescue
          FileUtils.rm persistence_file if File.exists? persistence_file
          raise $!
        end
      else
        begin
          fwt = FixWidthTable.new persistence_file, max_length, false
          fwt.add_point res
        rescue
          FileUtils.rm persistence_file
          raise $!
        end
      end

      fwt.read

      fwt
    else
      Log.debug "Loading #{ persistence_file }. Prefix = #{prefix}"

      fwt = FixWidthTable.new persistence_file, nil, nil

      fwt
    end
  end

  def self.persist(file, prefix = "", persistence_type = :string, options = {}, &block)
    options = Misc.add_defaults options, :persistence => true
    persistence =
      Misc.process_options options, :persistence

    filename = get_filename(file)

    o = options.dup
    o = 
      Misc.add_defaults o, :persistence_update => false, :persistence_file => nil, :filename => nil
    persistence_update, persistence_dir, persistence_file, filename =
      Misc.process_options o, :persistence_update, :persistence_dir, :persistence_file, :filename

    filename         ||= get_filename(file)
    persistence_file ||= get_persistence_file(filename, prefix, o.merge(:persistence_dir => persistence_dir))

    if persistence == :no_create
      persistence = false if not File.exists? persistence_file
    end

    if not persistence
      Log.low "Non Persistent Loading for #{filename}. Prefix: #{prefix}"
      yield file, options, filename
    else
      Log.low "Persistent Loading for #{filename}. Prefix: #{prefix}. Type #{persistence_type.to_s}"

      Misc.lock(persistence_file, file, prefix, options, block) do |persistence_file,file,prefix,options,block|

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
        when :fwt
          persist_fwt(file, prefix, options, &block)
        end
      end
    end
  end


end
