require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'rbbt/util/tc_hash'
require 'rbbt/util/tmpfile'
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
    if file =~ /(.*)#(.*)/ and File.exists? $1
      options.merge! Misc.string2hash $2
      file = $1
    end

    options = Misc.add_defaults options, :sep => "\t", :header_hash => "#"
    io = Open.open(file)
    line = io.gets
    io.close

    if line =~ /^#{options[:header_hash]}/
      line.chomp.sub(/^#{options[:header_hash]}/,'').split(options[:sep])
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
      :keep_empty       => true,
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
    line.chomp!

    if line =~ /^#{options[:header_hash]}/
      header_fields    = parse_fields(line, options[:sep])
      header_fields[0] = header_fields[0][(0 + options[:header_hash].length)..-1] # Remove initial hash character
      line = file.gets
    else
      header_fields = nil
    end
    
    id_pos = Misc.field_position(header_fields, options[:native])

    if options[:extra].nil?
      extra_pos = nil
      max_cols = 0
    else
      extra_pos = options[:extra].collect{|pos| Misc.field_position(header_fields, pos) }
    end

    #{{{ Process rest
    while line do
      line.chomp!

      line = options[:fix].call line if options[:fix]

      # Select and fix lines
      if (options[:exclude] and   options[:exclude].call(line)) or
         (options[:select]  and not options[:select].call(line))
         line = file.gets
         next
      end

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

      if options[:extra].nil? and not (options[:flatten] or options[:single])
        extra = parts 
        extra.delete_at(id_pos)
        max_cols = extra.size if extra.size > (max_cols || 0)
      else
        if extra_pos.nil?
          extra = parts
          extra.delete_at id_pos
        else
          extra = parts.values_at(*extra_pos)
        end
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

    if options[:keep_empty] and not max_cols.nil?
      data.each do |key,values| 
        new_values = values
        max_cols.times do |i|
          new_values[i] ||= [""]
        end
        data[key] = new_values
      end
    end


    # Save header information
    key_field = nil
    fields   = nil
    if header_fields && header_fields.any?
      key_field = header_fields[id_pos] 
      if extra_pos.nil?
        fields = header_fields
        fields.delete_at(id_pos) 
      else
        fields = header_fields.values_at(*extra_pos) 
      end
    end

    data.read if PersistenceHash === data

    [data, key_field, fields]
  end

  attr_accessor :data, :key_field, :fields, :list, :case_insensitive, :filename
  def initialize(file = {}, options = {})
    @case_insensitive = options[:case_insensitive] == true
    @list = ! (options[:flatten] == true || options[:single] == true || options[:unique] == true)

    case
    when TSV === file
      @filename         = file.filename
      @data             = file.data
      @key_field        = file.key_field
      @fields           = file.fields
      @case_insensitive = file.case_insensitive
      @list             = file.is_list
      return self
    when (Hash === file or PersistenceHash === file)
      @filename = "Hash:" + Digest::MD5.hexdigest(file.inspect)
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

  def self.fields_include(key_field, fields, field)
    return true if field == key_field or fields.include? field
    return false
  end

  def self.field_positions(key_field, fields, *selected)
    selected.collect do |sel|
      case
      when (sel.nil? or sel == :main or sel == key_field)
        -1
      when Integer === sel
        sel
      else
        Misc.field_position fields, sel
      end
    end
  end

  def fields_include(field)
    return TSV.fields_include key_field, fields, field
  end

  def field_positions(*selected)
    return nil if selected.nil? or selected == [nil]
    TSV.field_positions(key_field, fields, *selected)
  end

  def fields_at(*positions)
    return nil if fields.nil?
    return nil if positions.nil? or positions == [nil]
    (fields + [key_field]).values_at(*positions)
  end

  def through(new_key_field = nil, new_fields = nil, &block)
    new_key_position = (field_positions(new_key_field) || [-1]).first

    if new_key_position == -1

      if new_fields.nil? or new_fields == fields
        each &block 
        return [key_field, fields]
      else
        new_field_positions = field_positions(*new_fields)
        each do |key, values|
          yield key, values.values_at(*new_field_positions)
        end
        return [key_field, fields_at(*new_field_positions)]
      end

    else
      new_field_positions = field_positions(*new_fields)

      new_field_names = fields_at(*new_field_positions)
      if new_field_names.nil? and fields
        new_field_names = fields.dup
        new_field_names.delete_at new_key_position
        new_field_names.unshift key_field
      end

      each do |key, values|
        if list
          tmp_values = values + [[key]]
        else
          tmp_values = values + [key]
        end

        if new_field_positions.nil?
          new_values = values.dup
          new_values.delete_at new_key_position
          new_values.unshift [key] 
        else
          new_values = tmp_values.values_at(*new_field_positions)
        end

        tmp_values[new_key_position].each do |new_key|
          if new_field_names
            yield new_key, NamedArray.name(new_values, new_field_names)
          else
            yield new_key, new_values
          end
        end
      end
      return [(fields_at(new_key_position) || [nil]).first, new_field_names]
    end
  end
  
  def process(field)
    through do |key, values|
      values[field].replace yield(values[field], key, values) unless values[field].nil? 
    end
  end


  def reorder(new_key_field, new_fields = nil, options = {})
    options = Misc.add_defaults options
    return TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => case_insensitive)  if options[:persistence_file] and File.exists?(options[:persistence_file])

    new = {}
    new_key_field, new_fields = through new_key_field, new_fields do |key, values|
      if new[key].nil?
        new[key] = values
      else
        new[key] = new[key].zip(values)
      end
    end

    new.each do |key,values| 
      values.each{|list| list.flatten! if Array === list}
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

  def slice(new_fields, options = {})
    reorder(:main, new_fields)
  end

  def index(options = {})
    options = Misc.add_defaults options, :order => false

    if options[:persistence] and ! options[:persistence_file]
      options[:persistence_file] = TSV.get_persistence_file(filename, "index:#{ filename }_#{options[:field]}:", options)
    end

    if options[:persistence_file] and File.exists?(options[:persistence_file])
      return TSV.new(PersistenceHash.get(options[:persistence_file], false), :case_insensitive => options[:case_insensitive]) 
    end

    new = {}
    if options[:order]
      new_key_field, new_fields = through options[:field], options[:others] do |key, values|

        values.each_with_index do |list, i|
          next if list.nil? or list.empty?

          list = [list] unless Array === list

          list.each do |value|
            next if value.nil? or value.empty?
            value = value.downcase if options[:case_insensitive]
            new[value]    ||= []
            new[value][i + 1] ||= []
            new[value][i + 1] << key
          end
          new[key]    ||= []
          new[key][0] = key
        end

      end

      new.each do |key, values| 
        values.flatten!
        values.compact!
      end

    else
      new_key_field, new_fields = through options[:field], options[:others] do |key, values|
        new[key] ||= []
        new[key] << key 
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

  def smart_merge(other, match = nil, new_fields = nil)

    new_fields = [new_fields] if String === new_fields
    if self.fields and other.fields 
      common_fields = ([self.key_field] + self.fields)   & ([other.key_field] + other.fields)
      new_fields    ||= ([other.key_field] + other.fields) - ([self.key_field] + self.fields)

      common_fields.delete match if String === match
      common_fields.delete_at match if Integer === match

      this_common_field_positions   = self.field_positions *common_fields 
      other_common_field_positions  = other.field_positions *common_fields 
      other_new_field_positions     = other.field_positions *new_fields
    else
      nofieldinfo = true
    end

    case
    when TSV === match
      match_index = match
      matching_code_position = nil

    when Array === match
      match_index = match.first
      matching_code_position = field_positions(match.last).first

    when match =~ /^through:(.*)/
      through = $1
      if through =~ /(.*)#using:(.*)/
        through = $1
        matching_code_position = field_positions($2).first
      else
        matching_code_position = nil
      end
      index_fields = TSV.headers(through)
      target_field = index_fields.select{|field| other.fields_include field}.first
      Log.debug "Target Field: #{ target_field }"
      match_index = TSV.open_file(through).index(:field => target_field)

    when field_positions(match).first
      matching_code_position = field_positions(match).first
      match_index = nil
    end

    if matching_code_position.nil? and match_index.fields
      match_index.fields.each do |field| 
        if matching_code_position = field_positions(field).first
          break
        end
      end
    end

    if match_index and match_index.key_field == other.key_field
      other_index = nil
    else
      other_index = (match === String and other.fields_include(match)) ? 
        other.index(:other => match, :order => true) : other.index(:order => true)
    end

    each do |key,values|
      Log.debug "Key: #{ key }. Values: #{values * ", "}"
      if matching_code_position.nil? or matching_code_position == -1
        matching_codes = [key] 
      else
        matching_codes = values[matching_code_position]
        matching_codes = [matching_codes] unless  matching_codes.nil? or Array === matching_codes
      end
      Log.debug "Matching codes: #{matching_codes}"

      next if matching_codes.nil?

      matching_codes.each do |matching_code|
        if match_index
          if match_index[matching_code]
            matching_code_fix = match_index[matching_code].first
          else
            matching_code_fix = nil
          end
        else
          matching_code_fix = matching_code
        end

        Log.debug "Matching code (fix): #{matching_code_fix}"
        next if matching_code_fix.nil?

        if other_index
          Log.debug "Using other_index"
          other_codes = other_index[matching_code_fix]
        else
          other_codes = matching_code_fix
        end
        Log.debug "Other codes: #{other_codes}"

        next if other_codes.nil? or other_codes.empty?
        other_code = other_codes.first

        if nofieldinfo
          next if other[other_code].nil?
          if list
            other_values = [[other_code]] + other[other_code]
          else
            other_values = [other_code] + other[other_code]
          end
          other_values.delete_if do |list| 
            list = [list] unless Array === list
            list.collect{|e| case_insensitive ? e.downcase : e }.
                 select{|e| case_insensitive ? e == matching_code.downcase : e == matching_code }.any?
          end

          new_values = values + other_values 
        else
          if other[other_code].nil?
            if list
              other_values = [[]] * other.fields.length
            else
              other_values = [] * other.fields.length
            end
          else
            if list
              other_values = other[other_code] + [[other_code]]
            else
              other_values = other[other_code] + [other_code]
            end
          end
  

          new_values = values.dup

          if list
            this_common_field_positions.zip(other_common_field_positions).each do |tpos, opos|
              new_values_tops = new_values[tpos]

              if other.list
                new_values_tops += other_values[opos]
              else
                new_values_tops += [other_values[opos]]
              end

              new_values[tpos] = new_values_tops.uniq
            end
          end

          new_values.concat other_values.values_at *other_new_field_positions
        end

        self[key] = new_values
      end
    end

    self.fields = self.fields + new_fields unless nofieldinfo
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
      TSV.new(PersistenceHash.get(opt_index[:persistence_file], false), opt_index)
    else
      TSV.log "Creating index for #{ file }: #{opt_index[:persistence_file]}"
      data = TSV.new(file, opt_data)
      data.index(opt_index)
    end
  end

  def self.open_file(file)
    if file =~ /(.*?)#(.*)/
      file, options = $1, Misc.string2hash($2.to_s)
    else
      options = {}
    end

    TSV.new(file, options)
  end

end
