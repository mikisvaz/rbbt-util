require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'rbbt/util/tc_hash'
require 'digest'
require 'fileutils'

def add_defaults(options, defaults = {})
  new_options = options.dup
  defaults.each do |key, value|
    new_options[key] = value if new_options[key].nil?
  end
  new_options
end

class TSV
  class FieldNotFoundError < StandardError;end

  #{{{ Persistence

  PersistenceHash = TCHash

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
    File.join(CACHEDIR, prefix.gsub(/\s/,'_').gsub(/\//,'>') + Digest::MD5.hexdigest([file, options].inspect))
  end

  @debug = ENV['TSV_DEBUG'] == "true"
  def self.log(message)
    STDERR.puts message if @debug == true
  end

  def self.debug=(value)
    @debug = value
  end

  def self.headers(file, options = {})
    options = Misc.add_defaults options, :sep => "\t", :header_hash => "#"
    io = Open.open(file)
    line = io.gets
    io.close

    if line =~ /^#{options[:header_hash]}/
      line.sub(/^#{options[:header_hash]}/,'').split(options[:sep])
    else
      nil
    end
  end

  #{{{ Accesor Methods

  def keys
    @data.keys
  end

  def values
    @data.values
  end

  def size
    @data.size
  end

  # Write

  def []=(key, value)
    key = key.downcase if @case_insensitive
    @data[key] = value
  end


  def merge!(new_data)
    new_data.each do |key, value|
      self[key] = value
    end
  end

  # Read

  def follow(value)
    if String === value && value =~ /__Ref:(.*)/
      return self[$1]
    else
      value = NamedArray.name value, fields if Array === value and fields 
      value
    end
  end
  def [](key)
    if Array === key
      return @data[key] if @data[key] != nil
      key.each{|k| v = self[k]; return v unless v.nil?}
      return nil
    end

    key = key.downcase if @case_insensitive
    follow @data[key]
  end

  def values_at(*keys)
    keys.collect{|k|
      self[k]
    }
  end

  def each(&block)
    @data.each do |key, value|
      block.call(key, follow(value))
    end
  end

  def collect
    if block_given?
      @data.collect do |key, value|
        value = follow(value)
        key, values = yield key, value
      end
    else
      @data.collect do |key, value|
        [key, follow(value)]
      end
    end
  end

  def sort(&block)
    collect.sort(&block).collect{|p|
      key, value = p
      value = NamedArray.name value, fields if fields
      [key, value]
    }
  end

  def sort_by(&block)
    collect.sort_by &block
  end

  #{{{ Parsing
  
  def self.parse_fields(io, delimiter = "\t")
    return [] if io.nil?
    fields = io.split(delimiter, -1)
    fields
  end

  def self.zip_fields(list, fields = nil)
    return [] if list.nil? || list.empty?
    fields ||= list.fields if list.respond_to? :fields
    zipped = list[0].zip(*list[1..-1])
    zipped = zipped.collect{|v| NamedArray.name(v, fields)} if fields 
    zipped 
  end
  
  def self.parse(file, options = {})

    # Prepare options
    options = add_defaults options, 
      :sep              => "\t",
      :sep2             => "|",
      :native           => 0,
      :extra            => nil,
      :fix              => nil,
      :exclude          => nil,
      :select           => nil,
      :grep             => nil,
      :single           => false,
      :unique           => false,
      :flatten          => false,
      :overwrite        => false,
      :keep_empty       => false,
      :case_insensitive => false,
      :header_hash      => '#' ,
      :persistence_file => nil

    options[:extra]   = [options[:extra]] if options[:extra] != nil && ! (Array === options[:extra])
    options[:flatten] = true if options[:single]

    # Open data store
    data = options[:persistence_file].nil? ? {} : PersistenceHash.get(options[:persistence_file], true)


    #{{{ Process first line

    line = file.gets
    raise "Empty content" if line.nil?

    if line =~ /^#{options[:header_hash]}/
      header_fields    = parse_fields(line, options[:sep])
      header_fields[0] = header_fields[0][(0 + options[:header_hash].length)..-1] # Remove initial hash character
      line = file.gets
    else
      header_fields = nil
    end

    id_pos = Misc.field_position(header_fields, options[:native])

    if options[:extra].nil?
      parts = parse_fields(line.chomp, options[:sep])
      extra_pos = (0..(parts.length - 1 )).to_a
      extra_pos.delete(id_pos)  
    else
      extra_pos = options[:extra].collect{|pos| Misc.field_position(header_fields, pos) }
    end
    
    #{{{ Process rest
    while line do
      line.chomp!

      # Select and fix lines
      if (options[:exclude] and   options[:exclude].call(line)) or
         (options[:select]  and not options[:select].call(line))
         line = file.gets
         next
      end

      line = options[:fix].call line if options[:fix]

      ### Process line

      # Chunk fields
      parts = parse_fields(line, options[:sep])

      # Get next line
      line = file.gets

      # Get id field
      next if parts[id_pos].nil? || parts[id_pos].empty?
      ids = parse_fields(parts[id_pos], options[:sep2])
      ids.collect!{|id| id.downcase } if options[:case_insensitive]

      # Get extra fields
      
      if options[:extra].nil? and (options[:flatten] or options[:single])
        extra = parts 
        extra.delete_at(id_pos)
      else
        extra = parts.values_at(*extra_pos)
      end

      extra.collect!{|value| parse_fields(value, options[:sep2])}  
      extra.collect!{|values| values.first}       if options[:unique]
      extra.flatten!                              if options[:flatten]
      extra = extra.first                         if options[:single]

      if options[:overwrite]
        main_entry = ids.shift
        ids.each do |id|
          data[id] = "__Ref:#{main_entry}"  
        end

        data[main_entry] = extra
      else
        main_entry = ids.shift
        ids.each do |id|
          data[id] = "__Ref:#{main_entry}"
        end

        case
        when (options[:single] or options[:unique])
          data[main_entry] ||= extra
        when options[:flatten]
          if PersistenceHash === data
            data[main_entry] = (data[main_entry] || []).concat extra
          else
            data[main_entry] ||= []
            data[main_entry].concat extra
          end
        else
          entry = data[main_entry] || []
          while entry =~ /__Ref:(.*)/ do
            entry = data[$1]
          end

          extra.each_with_index do |fields, i|
            if fields.empty?
              next unless options[:keep_empty]
              fields = [""]
            end
            entry[i] ||= []
            entry[i] = entry[i].concat fields
          end

          data[main_entry] = entry
        end
      end
    end

    # Save header information
    key_field = nil
    fields   = nil
    if header_fields && header_fields.any?
      key_field = header_fields[id_pos] 
      fields = header_fields.values_at(*extra_pos) 
    end

    data.read if PersistenceHash === data

    [data, key_field, fields]
  end

  attr_accessor :data, :key_field, :fields, :case_insensitive, :filename
  def initialize(file = {}, options = {})
    @case_insensitive = options[:case_insensitive] == true

    case
    when TSV === file
      @filename         = file.filename
      @data             = file.data
      @key_field        = file.key_field
      @fields           = file.fields
      @case_insensitive = file.case_insensitive
      return self
    when Hash === file || PersistenceHash === file
      @filename = Hash
      @data = file
      return self
    when File === file
      @filename = File.expand_path file.path
    when String === file && File.exists?(file)
      @filename = File.expand_path file
      file = Open.open(file)
    when StringIO
    else 
      raise "File #{file} not found"
    end

    if options[:persistence]
      options.delete :persistence
      persistence_file = TSV.get_persistence_file @filename, "file:#{ @filename }:", options

      if File.exists? persistence_file
        TSV.log "Loading Persistence for #{ @filename } in #{persistence_file}"
        @data      = PersistenceHash.get(persistence_file, false)
        @key_field = @data.key_field
        @fields    = @data.fields
      else
        file = Open.grep(file, options[:grep]) if options[:grep]

        TSV.log "Persistent Parsing for #{ @filename } in #{persistence_file}"
        @data, @key_field, @fields = TSV.parse(file, options.merge(:persistence_file => persistence_file))
        @data.key_field            = @key_field
        @data.fields               = @fields
        @data.read
      end
    else
      TSV.log "Non-persistent parsing for #{ @filename }"
      file = Open.grep(file, options[:grep]) if options[:grep]
      @data, @key_field, @fields = TSV.parse(file, options)
    end

    file.close
    @case_insensitive = options[:case_insensitive] == true
  end

  def self.open_file(file)
    raise "File '#{file}' not in correct format: filename#options." \
      unless file.match(/(.*?)#(.*)/)

    file, options = $1, Misc.string2hash($2.to_s)

    TSV.new(file, options)
  end

  def to_s
    str = ""

    if fields
      str << "#" << key_field << "\t" << fields * "\t" << "\n"
    end

    each do |key, values|
      case
      when values.nil?
        str << key.dup << "\n"
      when Array === values.first
        str << key.dup <<  "\t" << values.collect{|list| (list || []) * "|"} * "\t" << "\n"
      else
        str << key.dup <<  "\t" << values * "\t" << "\n"
      end
    end

    str
  end

  #{{{ New

  def self.reorder(key_field, fields, new_key_field, new_fields)
    return [-1, nil, key_field, fields] if (new_key_field.nil? or new_key_field == :main) and new_fields.nil?
    return [new_key_field, nil, nil, nil] if Integer === new_key_field  and new_fields.nil? and fields.nil?

    new_fields = [new_fields] if String === new_fields
    if new_fields.nil? 
      new_fields = fields.dup
      new_fields.delete_at(Misc.field_position(fields, new_key_field))
      new_fields.unshift(key_field)
    end

    positions = new_fields.collect do |field|
      if field == :main or field == nil or field == key_field
        -1
      else
        Misc.field_position fields, field
      end
    end

    if new_key_field == :main
      new_key_field = key_field
      key_position  = -1 
    else
      key_position  = Misc.field_position fields, new_key_field
      new_key_field = fields[key_position]
    end

    [key_position, positions, new_key_field, new_fields]
  end


  def through(new_key_field = nil, new_fields = nil, &block)

    if new_key_field.nil? or new_key_field == :main or key_field == new_key_field
      if  new_fields.nil? or fields == new_fields
         each &block 
         return [key_field, fields]
      end

      positions = new_fields.collect{|field| Misc.field_position fields, field}

      if fields.nil?
        each do |key, values|
          yield key, values.values_at(*positions)
        end
        return [key_field, nil]
      else
        new_fields = fields.values_at *positions 
        each do |key, values|
          new_values = NamedArray.name values.values_at(*positions), new_fields
          yield key, new_values
        end
        return [key_field, new_fields]
      end

    else
      key_position, positions, new_key_field, new_fields = TSV.reorder(key_field, fields, new_key_field, new_fields)

      each do |key, values|
        new_values     = values.dup
        new_values.push [key]
        new_keys       = new_values[key_position]

        case 
        when positions
          new_values     = new_values.values_at(*positions) 
        when (positions.nil? and key_position != -1)
          new_values.delete_at key_position
          new_values.unshift(new_values.pop)
        end

        new_values = NamedArray.name(new_values, new_fields) if new_fields

        new_keys = [new_keys] unless Array === new_keys
        new_keys.each do |new_key|
          yield new_key, new_values
        end
      end

      return [new_key_field, new_fields]
    end
  end

  def reorder(new_key_field, new_fields = nil, options = {})
    options = Misc.add_defaults options
    return TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => case_insensitive) \
      if options[:persistence_file] and File.exists?(options[:persistence_file])

    new = {}
    new_key_field, new_fields = through new_key_field, new_fields do |key, values|
      if new[key].nil?
        new[key] = values
      else
        new[key] = new[key].zip(values)
      end
    end

    new.each do |key,values| 
      values.each{|list| list.flatten!}
    end

    if options[:persistence_file]
      reordered = TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => case_insensitive)
      reordered.merge! new
    else
      reordered = TSV.new(new, :case_insensitive => case_insensitive)
    end

    reordered.key_field = new_key_field
    reordered.fields    = new_fields

    reordered
  end

  def index(options = {})
    options = Misc.add_defaults options, :order => false
    return TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => options[:case_insensitive]) \
      if options[:persistence_file] and File.exists?(options[:persistence_file])

    new = {}
    if options[:order]
      new_key_field, new_fields = through options[:field], options[:others] do |key, values|

        values.each_with_index do |list, i|
          next if list.nil? or list.empty?
          list = [list] unless Array === list
          list.each do |value|
            next if value.nil? or value.empty?
            value = value.downcase if options[:case_insensitive]
            new[value] ||= []
            new[value][i] ||= []
            new[value][i] << key
          end
        end

      end

      new.each do |key, values| 
        values.flatten!
        values.compact!
      end
    else
      new_key_field, new_fields = through options[:field], options[:others] do |key, values|
        values.each do |list|
          next if list.nil? 
          if Array === list
            list.each do |value|
              value = value.downcase if options[:case_insensitive]
              new[value] ||= []
              new[value] << key
            end
          else
            next if list.empty?
            value = list
            value = value.downcase if options[:case_insensitive]
            new[value] ||= []
            new[value] << key
          end
        end
      end
    end

    if options[:persistence_file]
      index = TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => options[:case_insensitive])
      index.merge! new
    else
      index = TSV.new(new, :case_insensitive => options[:case_insensitive])
    end

    index.key_field = new_key_field
    index.fields    = new_fields
    index
  end
  
  def smart_merge(other, field = nil)

    if self.fields and other.fields 
      common_fields = self.fields & other.fields
      common_fields.delete field
      new_fields    = ([other.key_field] + other.fields) - self.fields - [self.key_field]
    else
      nofieldinfo = true
    end

    this_index  = self.index(:order => true, :field => field)
    if other.fields and not Integer === field and other.fields.include? field
      other_index = other.index(:others => field, :order => true)
    else
      other_index = other.index(:order => true)
    end

    each do |key, values|
      new_data = nil
      next if this_index[key].nil? 

      this_index[key].each do |common_id|
        if other_index[common_id] and other_index[common_id].any?
          if nofieldinfo
            new_data         = other[other_index[common_id]].dup
            new_data.unshift other_index[common_id].dup
            new_data.delete_if do |new_datavalues| new_datavalues.include? common_id end
          else
            new_data        = other[other_index[common_id]].dup
            new_data_fields = other.fields.dup                                    

            if other.key_field != field
              new_data.delete_at(Misc.field_position(other.fields, field))
              new_data.unshift other_index[common_id]

              new_data_fields.delete_at(Misc.field_position(other.fields, field)) 
              new_data_fields.unshift other.key_field                             
            end

            new_data = NamedArray.name(new_data, new_data_fields)
          end
          break
        end
      end

      next if new_data.nil?

      if nofieldinfo
        values.concat new_data
      else
        common_fields.each do |common_field|
          values[common_field] += new_data[common_field]
        end

        values.concat new_data.values_at(*new_fields)
      end

      self[key] = values
    end

    self.fields = self.fields + new_fields unless self.fields.nil?
  end

  #{{{ Helpers

  def self.index(file, options = {})
    opt_data = options.dup
    opt_index = options.dup
    opt_data.delete  :field
    opt_data.delete  :persistence
    opt_index.delete :persistence

    opt_data[:persistence] = true if options[:data_persistence]

    opt_index.merge! :persistence_file => get_persistence_file(file, "index:#{ file }_#{options[:field]}:", opt_index) if options[:persistence]

    if ! opt_index[:persistence_file].nil? && File.exists?(opt_index[:persistence_file])
      TSV.log "Reloading persistent index for #{ file }: #{opt_index[:persistence_file]}"
      TSV.new(PersistenceHash.get(opt_index[:persistence_file], false),opt_index)
    else
      TSV.log "Creating index for #{ file }: #{opt_index[:persistence_file]}"
      data = TSV.new(file, opt_data)
      data.index(opt_index)
    end
  end
end
