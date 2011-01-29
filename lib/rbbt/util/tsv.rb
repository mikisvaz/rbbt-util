require 'rbbt/util/misc'
require 'rbbt/util/open'
require 'rbbt/util/tc_hash'
require 'rbbt/util/tmpfile'
require 'rbbt/util/log'
require 'rbbt/util/persistence'
require 'digest'
require 'fileutils'

class TSV
  class FieldNotFoundError < StandardError;end

  module Field
    def ==(string)
      return false unless String === string
      self.sub(/#.*/,'').casecmp(string.sub(/#.*/,'')) == 0
    end
  end

  #{{{ Persistence

  CACHEDIR="/tmp/tsv_persistent_cache"
  FileUtils.mkdir CACHEDIR unless File.exist? CACHEDIR

  def self.cachedir=(cachedir)
    CACHEDIR.replace cachedir
    FileUtils.mkdir_p CACHEDIR unless File.exist? CACHEDIR
  end

  def self.cachedir
    CACHEDIR
  end

  
  #{{{ Headers and Field Stuff

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

  def self.fields_include(key_field, fields, field)
    return true if key_field == field or fields.include? field
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

  #{{{ Iteration, Merging, etc
  def through(new_key_field = nil, new_fields = nil, &block)
    new_key_position = (field_positions(new_key_field) || [-1]).first
    new_fields = [new_fields] if String === new_fields

    if new_key_position == -1

      if new_fields.nil? or new_fields == fields
        each &block 
        return [key_field, fields]
      else
        new_field_positions = field_positions(*new_fields)
        each do |key, values|
          if values.nil?
            yield key, nil
          else
            yield key, values.values_at(*new_field_positions)
          end
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
        if type == :double
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

        if not Array === tmp_values[new_key_position]
          yield tmp_values[new_key_position], NamedArray.name(new_values, new_field_names)  
        else
          tmp_values[new_key_position].each do |new_key|
            if new_field_names
              yield new_key, NamedArray.name(new_values, new_field_names)
            else
              yield new_key, new_values
            end
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
    return TSV.new(Persistence::TSV.get(options[:persistence_file], false), :case_insensitive => case_insensitive)  if options[:persistence_file] and File.exists?(options[:persistence_file])

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
      reordered = TSV.new(Persistence::TSV.get(options[:persistence_file], false), :case_insensitive => case_insensitive)
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

  def add_field(name = nil)
    each do |key, values|
      self[key] = values + [yield(key, values)]
    end

    if fields != nil
      new_fields = fields + [name]
      self.fields = new_fields
    end
  end

  def select(method)
    new = TSV.new({})
    new.key_field = key_field
    new.fields    = fields.dup
    new.type      = type
    new.filename  = filename + "#Select: #{method.inspect}"
    new.case_insensitive  = case_insensitive
    
    case
    when Array === method
      through do |key, values|
        new[key] = values if ([key,values].flatten & method).any?
      end
    when Regexp === method
      through do |key, values|
        new[key] = values if [key,values].flatten.select{|v| v =~ method}.any?
      end
    when String === method
      through do |key, values|
        new[key] = values if [key,values].flatten.select{|v| v == method}.any?
      end
    when Hash === method
      key  = method.keys.first
      method = method.values.first
      case
      when (Array === method and (:main == key or key_field == key))
        method.each{|item| if values = self[item]; then  new[item] = values; end}
      when Array === method
        through :main, key do |key, values|
          new[key] = self[key] if (values.flatten & method).any?
        end
      when Regexp === method
        through :main, key do |key, values|
          new[key] = self[key] if values.flatten.select{|v| v =~ method}.any?
        end
      when String === method
        through :main, key do |key, values|
          new[key] = self[key] if values.flatten.select{|v| v == method}.any?
        end
      end
    end


    new
  end

  def index(options = {})
    options = Misc.add_defaults options, :order => false, :persistence => false

    new, extra = Persistence.persist(filename, :Index, :tsv, options) do |filename, options|
      new = {}
      if options[:order]
        new_key_field, new_fields = through options[:target], options[:others] do |key, values|

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
        new_key_field, new_fields = through options[:target], options[:others] do |key, values|
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

      [new, {:key_field => new_key_field, :fields => new_fields, :type => :double, :case_insensitive => options[:case_insensitive]}]
    end

    new = TSV.new(new)
    new.filename = "Index: " + filename + options.inspect
    new.fields = extra[:fields]
    new.key_field = extra[:key_field]
    new.case_insensitive = extra[:case_insensitive]
    new.type = extra[:type]
    new
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
          if type == :double
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
            if type == :double
              other_values = [[]] * other.fields.length
            else
              other_values = [] * other.fields.length
            end
          else
            if type == :double
              other_values = other[other_code] + [[other_code]]
            else
              other_values = other[other_code] + [other_code]
            end
          end
  

          new_values = values.dup

          if type == :double
            this_common_field_positions.zip(other_common_field_positions).each do |tpos, opos|
              new_values_tops = new_values[tpos]

              if other.type == :double
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
  

  def self.field_matches(tsv, values)
    if values.flatten.sort[0..9].compact.collect{|n| n.to_i} == (1..10).to_a
      return {}
    end

    key_field = tsv.key_field
    fields    = tsv.fields

    field_values = {} 
    fields.each{|field| 
      field_values[field] = []
    }

    tsv.through do |key,entry_values|
      fields.zip(entry_values).each do |field,entry_field_values|
        field_values[field].concat entry_field_values
      end
    end

    field_values.each do |field,field_value_list|
      field_value_list.replace(values & field_value_list.flatten.uniq)
    end

    field_values[key_field] = values & tsv.keys 

    field_values
  end

  def field_matches(values)
    TSV.field_matches(self, values)
  end



  #{{{ Helpers

  def self.index(file, options = {})
    options = Misc.add_defaults options, :data_persistence => true, :persistence => true
    persistence, persistence_file = Misc.process_options options, :persistence, :persistence_file
    options[:persistence], options[:persistence_file] =  options.values_at :data_persistence, :data_persistence_file
    options.delete :data_persistence
    options.delete :data_persistence_file

    index, extra = Persistence.persist(file, :Index, :tsv, options) do |file, options, filename|
      TSV.new(file, :double, options).index
    end
    index
  end

  def self.index2(file, options = {})
    opt_data = options.dup
    opt_index = options.dup
    opt_data.delete  :field
    opt_data.delete  :persistence
    opt_index.delete :persistence

    opt_data[:persistence] = true if options[:data_persistence]

    opt_index.merge! :persistence_file => get_persistence_file(file, "index:#{ file }_#{options[:field]}:", opt_index) if options[:persistence]

    if ! opt_index[:persistence_file].nil? && File.exists?(opt_index[:persistence_file])
      Log.low "Reloading persistent index for #{ file }: #{opt_index[:persistence_file]}"
      TSV.new(Persistence::TSV.get(opt_index[:persistence_file], false), opt_index)
    else
      Log.low "Creating index for #{ file }: #{opt_index[:persistence_file]}"
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

  #{{{ Accesor Methods
  attr_accessor :filename, :type, :case_insensitive, :key_field, :fields, :data

  def fields
    return nil if @fields.nil?
    fields = @fields
    fields.each do |f| f.extend Field end if Array === fields
    fields
  end

  def fields=(new_fields)
    @fields = new_fields
    if Persistence::TSV === @data
      @data.fields = new_fields
    end
  end



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
    return nil if value.nil?
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

    key = key.downcase if @case_insensitive and key !~ /^__Ref:/
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

  def values_to_s(values)
      case
      when (values.nil? and fields.nil?)
        "\n"
      when (values.nil? and not fields.nil?)
        "\t" << ([""] * fields.length) * "\t" << "\n"
      when (not Array === values)
        "\t" << values.to_s << "\n"
      when Array === values.first
        "\t" << values.collect{|list| (list || []) * "|"} * "\t" << "\n"
      else
        "\t" << values * "\t" << "\n"
      end
  end

  def to_s(keys = nil)
    str = ""

    if fields
      str << "#" << key_field << "\t" << fields * "\t" << "\n"
    end

    if keys.nil?
      each do |key, values|
        key = key.to_s if Symbol === key
        str << key.dup << values_to_s(values)
      end
    else
      keys.zip(values_at(*keys)).each do |key, values|
        key = key.to_s if Symbol === key
        str << key.dup << values_to_s(values)
      end
    end

    str
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

  def self.key_order(file, options = {})
    # Prepare options
    options = add_defaults options, 
      :sep              => "\t",
      :sep2             => "|",
      :native           => 0,
      :fix              => nil,
      :exclude          => nil,
      :select           => nil,
      :grep             => nil,
      :case_insensitive => false,
      :header_hash      => '#'

    options[:extra]   = [options[:extra]] if options[:extra] != nil && ! (Array === options[:extra])

    if String === file and File.exists? file
      file = File.open(file)
    end

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

    ids = []
    #{{{ Process rest
    while line do
      line.chomp!

      line = options[:fix].call line if options[:fix]
      break if not line

      # Select and fix lines
      if line.empty?                                               or
         (options[:exclude] and     options[:exclude].call(line))  or
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
      ids << parts[id_pos]
    end

    ids
  end

  def self.parse_header(stream, sep, header_hash)
    fields, key_field = nil
    options = {}
    
    line = stream.gets

    if line and line =~ /^#{header_hash}: (.*)/
      options = Misc.string2hash $1
      line = stream.gets
    end

    sep = options[:sep] if options[:sep]

    if line and line =~ /^#{header_hash}/
      line.chomp!
      fields = parse_fields(line, sep)
      key_field = fields.shift
      key_field = key_field[(0 + header_hash.length)..-1] # Remove initial hash character
      line = stream.gets
    end

    raise "Empty content" if line.nil?
    return key_field, fields, options, line
  end

  def self.parse(stream, options = {})
    # Prepare options
    options = Misc.add_defaults options, 
      :case_insensitive => false,
      :type             => :double,

      :merge            => false,
      :keep_empty       => true,
      :cast             => nil,

      :sep              => "\t",
      :sep2             => "|",
      :header_hash      => '#',

      :key              => 0,
      :fields           => nil,

      :fix              => nil,
      :exclude          => nil,
      :select           => nil,
      :grep             => nil


    sep, header_hash =
      Misc.process_options options, :sep,  :header_hash

    key_field, other_fields, more_options, line = TSV.parse_header(stream, sep, header_hash)

    sep     = more_options[:sep] if more_options[:sep]
    options = Misc.add_defaults options, more_options
    sep2    = Misc.process_options options, :sep2

    key, others =
      Misc.process_options options, :key, :others

    if key_field.nil?
      key_pos      = key
      key_field, fields = nil
    else
      all_fields = [key_field].concat other_fields

      key_pos   = Misc.field_position(all_fields, key)

      if String === others or Symbol === others
        others = [others]
      end

      if others.nil?
        other_pos    = (0..(all_fields.length - 1)).to_a
        other_pos.delete key_pos
      else
        other_pos = Misc.field_position(all_fields, *others)
      end

      key_field = all_fields[key_pos]
      fields    = all_fields.values_at *other_pos
    end

    case_insensitive, type, merge, keep_empty, cast = 
      Misc.process_options options, :case_insensitive, :type, :merge, :keep_empty, :cast
    fix, exclude, select, grep = 
      Misc.process_options options, :fix, :exclude, :select, :grep 
    
    #{{{ Process rest
    data = {}
    single = type.to_sym != :double
    max_cols = 0
    while line do
      line.chomp!

      line = fix.call line if fix
      break if not line

      if header_hash and line =~ /^#{header_hash}/
        line = stream.gets
        next
      end

      if line.empty?                           or
         (exclude and     exclude.call(line))  or
         (select  and not select.call(line))

         line = stream.gets
         next
      end

      # Chunk fields
      parts = parse_fields(line, sep)

      # Get next line
      line = stream.gets

      # Get id field
      next if parts[key_pos].nil? || parts[key_pos].empty?
     
      if single
        ids = parse_fields(parts[key_pos], sep2)
        ids.collect!{|id| id.downcase} if case_insensitive
        
        id = ids.shift
        ids.each do |id2| data[id2] = "__Ref:#{id}"  end

        if key_field.nil?
          other_pos    = (0..(parts.length - 1)).to_a
          other_pos.delete key_pos
        end

        extra = parts.values_at(*other_pos).collect{|f| parse_fields(f, sep2).first}
        extra.collect! do |elem| 
          case
          when String === cast
            elem.send(cast)
          when Proc === cast
            cast.call elem
          end
        end if cast

        max_cols = extra.size if extra.size > (max_cols || 0)
        case type
        when :list
          data[id] = extra unless data.include? id
        when :flat
          data[id] = extra.flatten unless data.include? id  
        when :single
          data[id] = extra.flatten.first unless data.include? id  
        end
 
      else
        ids = parse_fields(parts[key_pos], sep2)
        ids.collect!{|id| id.downcase} if case_insensitive

        id = ids.shift
        ids.each do |id2| data[id2] = "__Ref:#{id}"  end

        if key_field.nil?
          other_pos    = (0..(parts.length - 1)).to_a
          other_pos.delete key_pos
        end

        extra = parts.values_at(*other_pos).collect{|f| parse_fields(f, sep2)}
        extra.collect! do |list| 
          case
          when String === cast
            list.collect{|elem| elem.send(cast)}
          when Proc === cast
            list.collect{|elem| cast.call elem}
          end
        end if cast

        max_cols = extra.size if extra.size > (max_cols || 0)
        if merge
          data[id] = extra unless data.include? id
        else
          if not data.include? id
            data[id] = extra
          else
            entry = data[id]
            while entry =~ /__Ref:(.*)/ do entry = data[$1] end
            extra.each_with_index do |f, i|
              if f.empty?
                next unless keep_empty
                f= [""]
              end
              entry[i] ||= []
              entry[i] = entry[i].concat f
            end
            data[id] = entry
          end
        end
      end
    end

    if keep_empty and max_cols > 0
      data.each do |key, values| 
        next if values =~ /__Ref:/
        new_values = values
        max_cols.times do |i|
          if type == :double
            new_values[i] = [""] if new_values[i].nil? or new_values[i].empty?
          else
            new_values[i] = "" if new_values[i].nil?
          end
        end
        data[key] = new_values
      end
    end

    [data, {:key_field => key_field, :fields => fields, :type => type, :case_insensitive => case_insensitive}]
  end
 
  def self.parse2(data, file, options = {})

    # Prepare options
    options = Misc.add_defaults options, 
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
      :merge            => false,
      :flatten          => false,
      :keep_empty       => true,
      :case_insensitive => false,
      :header_hash      => '#' ,
      :cast             => nil,
      :persistence_file => nil
       

    options[:unique]  = options[:uniq] if options[:unique].nil?
    options[:extra]   = [options[:extra]] if options[:extra] != nil && ! (Array === options[:extra])
    options[:flatten] = true if options[:single]

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
      break if not line

      if options[:header_hash] && line =~ /^#{options[:header_hash]}/
        line = file.gets
        next
      end

      # Select and fix lines
      if line.empty?                                               or
         (options[:exclude] and     options[:exclude].call(line))  or
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

      if options[:cast]
        if Array === extra[0]
          e = extra
        else
          e = [extra]
        end

        e.each do |list|
          case
          when String === options[:cast]
            list.collect!{|elem| elem.send(options[:cast])}
          when Proc === options[:cast]
            list.collect!{|elem| options[:cast].call elem}
          end
        end
      end

      main_entry = ids.shift
      ids.each do |id| data[id] = "__Ref:#{main_entry}"  end

      case
      when (options[:single] or options[:unique] or not options[:merge])
        data[main_entry] = extra unless data.include? main_entry
      when options[:flatten]
        entry = data[main_entry]

        if entry.nil?
          data[main_entry] = extra
        else
          while entry =~ /__Ref:(.*)/ do entry = data[$1] end
          if Persistence::TSV === data
            data[main_entry] = entry.concat extra
          else
            data[main_entry].concat extra
          end
        end
      else
        entry = data[main_entry]
        if entry.nil?
          data[main_entry] = extra
        else
          while entry =~ /__Ref:(.*)/ do entry = data[$1] end
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

    data.read if Persistence::TSV === data

    [key_field, fields]
  end
  def initialize(file = {}, type = :double, options = {})
    if Hash === type
      options = type 
      type    = :double 
    end

    if String === file and file =~/(.*?)#(.*)/ and File.exists? $1
      options = Misc.add_defaults options, Misc.string2hash($2) 
      file = $1
    end

    options = Misc.add_defaults options, :persistence => false, :case_insensitive => false, :type => type

    @filename = Misc.process_options options, :filename
    @filename ||= case
                  when (String === file and File.exists? file)
                    File.expand_path file
                  when File === file
                    File.expand_path file.path
                  else
                    Digest::MD5.hexdigest(file.inspect)
                  end

    if block_given?
      @data, extra = Persistence.persist(@filename, :TSV, :tsv, options) do |filename, options| yield filename, options end
    else
      @data, extra = Persistence.persist(@filename, :TSV, :tsv, options) do |filename, options|
        data, extra = nil
        case
        when String === file
          File.open(file) do |f|
            data, extra = TSV.parse(f, options)
          end
        when File === file
          data, extra = TSV.parse(file, options)
        when Hash === file
          data = file
          extra = {:case_insensitive => options[:case_insensitive], :type => type}
        end

        [data, extra]
      end
    end

    @type             = extra[:type]
    @key_field        = extra[:key_field]
    @fields           = extra[:fields]
    @case_insensitive = extra[:case_insensitive]
  end

  def initialize2(file = {}, options = {})
    options = Misc.add_defaults options
    options[:persistence] = true if options[:persistence_file]

    if String === file && file =~ /(.*?)#(.*)/
      file, file_options = $1, $2
      options = Misc.add_defaults file_options, options
    end

    @case_insensitive = options[:case_insensitive] == true
    @list = ! (options[:flatten] == true || options[:single] == true || options[:unique] == true)

    case
    when TSV === file
      Log.low "Copying TSV"
      @filename         = file.filename

      if options[:persistence] and not Persistence::TSV === file.data
        persistence_file = options.delete(:persistence_file) || TSV.get_persistence_file(@filename, "file:#{ @filename }:", options)
        Log.low "Making persistance #{ persistence_file }"
        @data = TCHash.get(persistence_file)
        @data.merge! file
        @data.key_field        = file.key_field
        @data.fields           = file.fields
      else
        @data             = file.data
      end

      @key_field        = file.key_field
      @fields           = file.fields
      @case_insensitive = file.case_insensitive
      @list             = file.list
      return self
    when Hash === file
      Log.low "Encapsulating Hash in TSV object"
      @filename = "Hash:" + Digest::MD5.hexdigest(file.inspect)
      if options[:persistence] 
        persistence_file = options.delete(:persistence_file) || TSV.get_persistence_file(@filename, "file:#{ @filename }:", options)
        Log.low "Making persistance #{ persistence_file }"
        @data = TCHash.get(persistence_file)
        @data.merge! file
      else
        @data = file
      end
      return self
    when Persistence::TSV === file
      Log.low "Encapsulating Persistence::TSV"
      @filename = "Persistence::TSV:" + Digest::MD5.hexdigest(file.inspect)
      @data             = file
      @key_field        = file.key_field
      @fields           = file.fields
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
      persistence_file = options.delete(:persistence_file) || TSV.get_persistence_file(@filename, "file:#{ @filename }:", options)

      if File.exists? persistence_file
        Log.low "Loading Persistence for #{ @filename } in #{persistence_file}"
        @data      = Persistence::TSV.get(persistence_file, false)
        @key_field = @data.key_field
        @fields    = @data.fields
      else
        @data = Persistence::TSV.get(persistence_file, true)
        file = Open.grep(file, options[:grep]) if options[:grep]

        Log.low "Persistent Parsing for #{ @filename } in #{persistence_file}"
        @key_field, @fields = TSV.parse(@data, file, options.merge(:persistence_file => persistence_file))
        @data.key_field            = @key_field
        @data.fields               = @fields
        @data.read
      end
    else
      Log.low "Non-persistent parsing for #{ @filename }"
      @data = {}
      file = Open.grep(file, options[:grep]) if options[:grep]
      @key_field, @fields = TSV.parse(@data, file, options)
    end

    file.close
    @case_insensitive = options[:case_insensitive] == true
  end

end

#{{{ CacheHelper
require 'rbbt/util/cachehelper'
module CacheHelper
  def self.tsv_cache(name, key = [])
    cache_file = CacheHelper.build_filename name, key

    if File.exists? cache_file
      Log.debug "TSV cache file '#{cache_file}' found"
      hash = TCHash.get(cache_file)
      TSV.new(hash)
    else
      Log.debug "Producing TSV cache file '#{cache_file}'"
      data = yield
      TSV.new(data,  :persistence_file => cache_file)
    end
  end
end
