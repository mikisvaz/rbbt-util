require 'yaml'
require 'rbbt/annotations'
require 'rbbt/tsv/dumper'
require 'set'

module TSV

  TSV_SERIALIZER = YAML
  SERIALIZED_NIL = TSV_SERIALIZER.dump nil

  attr_accessor :unnamed, :serializer_module, :entity_options, :entity_templates

  def info
    {:key_field => key_field, :fields => fields, :namespace => namespace, :entity_options => entity_options, :type => type, :filename => filename, :identifiers => identifiers, :unnamed => unnamed, :cast => cast}.delete_if{|k,v| v.nil? }
  end

  def annotate(tsv)
    TSV.setup(tsv, info)
  end

  def entity_options
    if @entity_options.nil?
      @entity_options = namespace ? {:namespace => namespace, :organism => namespace} : {}
      @entity_templates = nil
    end
    @entity_options
  end

  def entity_options=(options)
    @entity_options = options || {}
    if namespace
      @entity_options[:organism] ||= namespace
      @entity_options[:namespace] ||= namespace
    end
    @entity_templates = nil
  end


  def entity_templates
    @entity_templates ||= {}
  end

  def prepare_entity(entity, field, options = {})
    return entity if entity.nil?
    return entity unless defined? Entity
    entity = entity if options.delete :dup_array
    if (template = entity_templates[field]) and template.respond_to?(:annotate)
      if String === entity or Array === entity
        entity = entity.dup if entity.frozen? 
        template.annotate entity
        entity.extend AnnotatedArray if Array === entity
      end
      entity
    else
      if entity_templates.include? field
        entity
      else
        template = Misc.prepare_entity("TEMPLATE", field, options)
        if template.respond_to?(:annotate)
          entity_templates[field] = template
          if String === entity or Array === entity
            entity = entity.dup if entity.frozen? 
            template.annotate entity
            entity.extend AnnotatedArray if Array === entity
          end
          entity
        else
          entity_templates[field] = nil
          entity
        end
      end
    end
  end

  def setup_array(*args)
    res = NamedArray.setup(*args)
    res.instance_variable_set(:@entity_templates, entity_templates)
    res
  end

  def with_unnamed
    saved_unnamed = @unnamed 
    @unnamed = true
    res = yield
    @unnamed = saved_unnamed
    res
  end

  def with_monitor(value = true)
    saved_monitor = @monitor
    @monitor = value.nil? ? false : value
    res = yield
    @monitor = saved_monitor
    res
  end

  def close
    begin
      super
    rescue Exception
      self
    end
  end

  def read(force = false)
    begin
      super
    rescue Exception
      Log.exception $!
      @writable = false
      self
    end
  end

  def write(force = false)
    begin
      super
    rescue Exception
      @writable = true
      self
    end
  end

  def write?
    @writable
  end

  def self._extended(data)
    if not data.respond_to? :write
      class << data
        attr_accessor :writable

      end
    end
  end

  #{{{ TSV ENTRIES and ENTRY_KEYS

  KEY_PREFIX = "__tsv_hash_"
  ENTRIES = []
  ENTRY_KEYS = Set.new
  NIL_VALUE = "NIL_VALUE"

  def load_entry_value(value)
    return value unless respond_to? :persistence_path
    (value.nil? or value == SERIALIZED_NIL) ? nil : TSV_SERIALIZER.load(value)
  end

  def dump_entry_value(value)
    return value unless respond_to? :persistence_path
    (value.nil? or value == SERIALIZED_NIL) ? SERIALIZED_NIL : TSV_SERIALIZER.dump(value)
  end

  def self.entry(*entries)
    entries = entries.collect{|entry| entry.to_s}
    ENTRIES.concat entries
    entries.each do |entry|
      key = KEY_PREFIX + entry
      ENTRY_KEYS << key
      var_name = ("@" << entry).to_sym

      TSV.send(:define_method, entry) do
        return instance_variable_get(var_name) if instance_variables.include? var_name
        svalue = self.send(:[], key, :entry_key)
        value = load_entry_value(svalue)
        instance_variable_set(var_name, value)
        value
      end

      TSV.send(:define_method, entry + "=") do |value|
        instance_variable_set(var_name, value)
        value = value.to_s if Path === value
        self.send(:[]=, key, dump_entry_value(value), :entry_key)
        value
      end

    end
  end

  entry :key_field, 
    :type,
    :fields,
    :cast,
    :identifiers,
    :namespace,
    :filename,
    :serializer

  attr_reader :serializer_module

  def serializer=(serializer)
    @serializer = serializer
    self.send(:[]=, KEY_PREFIX + 'serializer', dump_entry_value(serializer), :entry_key)
    @serializar_module = serializer.nil? ? TSV::CleanSerializer : SERIALIZER_ALIAS[serializer.to_sym]
  end


  def serializer_module
    @serializer_module ||= begin
                             serializer = self.serializer
                             mod = serializer.nil? ? TSV::CleanSerializer : SERIALIZER_ALIAS[serializer.to_sym]
                             raise "No serializer_module for: #{ serializer.inspect }" if mod.nil?
                             mod
                           end
  end

  def empty?
    length == 0
  end

  #{{{ GETTERS AND SETTERS

  def prepare_value(key, value)
    value = @serializer_module.load(value) if @serializer_module and not TSV::CleanSerializer == @serializer_module

    return value if @unnamed or fields.nil?

    case type
    when :double, :list
      setup_array value, fields, key, entity_options, entity_templates
    when :flat, :single
      begin value = value.dup; rescue; end if value.frozen?

      value = prepare_entity(value, fields.first, entity_options)
    end
    value
  end

  def [](key, clean = false)
    value = super(key)
    return value if clean or value.nil?
    @serializer_module ||= self.serializer_module

    if MultipleResult === value
      res = value.collect{|v| prepare_value key, v }
      res.extend MultipleResult
      res
    else
      prepare_value key, value
    end
  end

  def []=(key, value, clean = false)
    return super(key, value) if clean or value.nil? or TSV::CleanSerializer == self.serializer_module 
    super(key, @serializer_module.dump(value))
  end

  def zip_new(key, values)
    values = [values] unless Array === values
    case type
    when :double
      if self.include? key
        new = []
        self[key, true].each_with_index do |v,i|
          _v = values[i]
          case _v
          when Array
            _n = v + _v
          else
            _n = v << _v
          end
          new << _n
        end
        self[key] = new
      else
        self[key] = Array === values.first ? values.dup : values.collect{|v| [v] }
      end
    when :flat
      if self.include? key
        self[key] = (self[key] + values).uniq
      else
        self[key] = values
      end
    else
      raise "Cannot zip_new for type: #{type}"
    end
  end

  def keys
    keys = super - ENTRY_KEYS.to_a
    return keys if @unnamed or key_field.nil?

    prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
  end

  def values
    values = chunked_values_at(keys)
    return values if @unnamed or fields.nil?

    case type
    when :double, :list
      values.each{|value| setup_array value, fields, nil, entity_options}
    when :single
      values = prepare_entity(values, fields.first, entity_options)
    when :flat
      values = values.collect{|v| prepare_entity(v, fields.first, entity_options)}
    end
      
    values
  end

  def each
    fields = self.fields

    serializer_module = self.serializer_module
    super do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless value.nil? or serializer_module.nil? or TSV::CleanSerializer == serializer_module

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not fields.nil? 
          case type
          when :double, :list
            setup_array value, fields, key, entity_options, entity_templates if Array == value
          when :flat, :single
            prepare_entity(value, fields.first, entity_options)
          end
        end
        key = prepare_entity(key, key_field, entity_options)
      end

      yield key, value if block_given?
      [key, value]
    end
  end

  def collect
    serializer_module = self.serializer_module
    super do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless serializer_module.nil? or TSV::CleanSerializer == serializer_module

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not fields.nil? 
          case type
          when :double, :list
            setup_array value, fields, key, entity_options if Array === value 
          when :flat, :single
            value = prepare_entity(value, fields.first, entity_options)
          end
        end
        key = prepare_entity(key, key_field, entity_options)
      end

      if block_given?
        yield key, value
      else
        [key, value]
      end
    end
  end

  def size
    super - ENTRY_KEYS.select{|k| self.include? k}.length
  end

  def length
    keys.length
  end

  def values_at(*keys)
    keys.collect do |key|
      self[key]
    end
  end

  def chunked_values_at(keys, max = 5000)
    Misc.ordered_divide(keys, max).inject([]) do |acc,c|
      new = self.values_at(*c)
      new.annotate acc if new.respond_to? :annotate and acc.empty?
      acc.concat(new)
    end
  end

  #{{{ Sorting

  def sort_by(field = nil, just_keys = false, &block)
    field = :all if field.nil?

    if field == :all
      elems = collect
    else
      elems = []
      case type
      when :single
        through :key, field do |key, field|
          elems << [key, field]
        end
      when :list, :flat
        through :key, field do |key, fields|
          elems << [key, fields.first]
        end
      when :double
        through :key, field do |key, fields|
          elems << [key, fields.first]
        end
      end
    end

    if not block_given?
      if fields == :all
        if just_keys
          keys = elems.sort_by{|key, value| key }.collect{|key, values| key}
          keys = prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
        else
          elems.sort_by{|key, value| key }
        end
      else
        sorted = elems.sort do |a, b| 
          a_value = a.last
          b_value = b.last
          a_empty = a_value.nil? or (a_value.respond_to?(:empty?) and a_value.empty?)
          b_empty = b_value.nil? or (b_value.respond_to?(:empty?) and b_value.empty?)
          case
          when (a_empty and b_empty)
            0
          when a_empty
            -1
          when b_empty
            1
          when Array === a_value
            if a_value.length == 1 and b_value.length == 1
              a_value.first <=> b_value.first
            else
              a_value.length <=> b_value.length
            end
          else
            a_value <=> b_value
          end
        end
        if just_keys
          keys = sorted.collect{|key, value| key}
          keys = prepare_entity(keys, key_field, entity_options.merge(:dup_array => true)) unless @unnamed
          keys
        else
          sorted.collect{|key, value| [key, self[key]]}
        end
      end
    else
      if just_keys
        keys = elems.sort_by(&block).collect{|key, value| key}
        keys = prepare_entity(keys, key_field, entity_options.merge(:dup_array => true)) unless @unnamed
        keys
      else
        elems.sort_by(&block).collect{|key, value| [key, self[key]]}
      end
    end
  end

  def tsv_sort(&block)
    collect.sort &block
  end

  # Starts in page 1
  def page(pnum, psize, field = nil, just_keys = false, reverse = false, &block)
    pstart = psize * (pnum - 1)
    pend = psize * pnum - 1
    field = :key if field == "key"
    keys = sort_by(field || :key, true, &block)
    keys.reverse! if reverse

    if just_keys
      keys[pstart..pend]
    else
      select :key => keys[pstart..pend]
    end
  end


  def fields
    #@fields ||= TSV_SERIALIZER.load(self.send(:[], "__tsv_hash_fields", :entry_key) || SERIALIZED_NIL)
    @fields ||= load_entry_value(self.send(:[], "__tsv_hash_fields", :entry_key))
    if true or @fields.nil? or @unnamed
      @fields
    else
      @named_fields ||= NamedArray.setup @fields, @fields, nil, entity_options, entity_templates
    end
  end

  def namespace=(value)
    self.send(:[]=, "__tsv_hash_namespace", dump_entry_value(value), true)
    @namespace = value
  end

  def fields=(value)
    clean = true
    self.send(:[]=, "__tsv_hash_fields", dump_entry_value(value), clean)
    @fields = value
    @named_fields = nil
  end

  def self.zip_fields(list, fields = nil)
    return [] if list.nil? || list.empty?
    fields ||= list.fields if list.respond_to? :fields
    zipped = list[0].zip(*list[1..-1])
    zipped = zipped.collect{|v| setup_array(v, fields)} if fields 
    zipped 
  end

  def identifier_files
    case
    when (identifiers and TSV === identifiers)
      [identifiers]
    when (identifiers and Array === identifiers)
      case
      when (TSV === identifiers.first or identifiers.empty?)
        identifiers
      else
        identifiers.collect{|f| Path === f ? f : Path.setup(f)}
      end
    when identifiers
      [ Path === identifiers ? identifiers : Path.setup(identifiers) ]
    when Path === filename
      filename.identifier_files
    when filename
      Path.setup(filename.dup).identifier_files
    else
      []
    end
  end

  def options
    options = {}
    ENTRIES.each do |entry|
      options[entry.to_sym] = self.send(entry)
    end
    IndiferentHash.setup options
  end


  def all_fields
    return nil if key_field.nil? or fields.nil?
    [key_field] + fields
  end

  def values_to_s(values)
    case values
    when nil
      if fields.nil? or fields.empty?
        "\n"
      else
        "\t" << ([""] * fields.length) * "\t" << "\n"
      end
    when Array
      "\t" << values.collect{|v| Array === v ? v * "|" : v} * "\t" << "\n"
    else
      "\t" << values.to_s << "\n"
    end
  end

  def dumper_stream(keys = nil, no_options = false)
    TSV::Dumper.stream self do |dumper|
      dumper.init unless no_options
      begin
        if keys
          keys.each do |key|
            dumper.add key, self[key]
          end
        else
          with_unnamed do
            each do |k,v|
              dumper.add k, v
            end
          end
        end
      rescue Exception
        Log.exception $!
        raise $!
      end
      dumper.close
    end
  end

  def to_s(keys = nil, no_options = false)
    if FalseClass === keys or TrueClass === keys
      no_options = keys
      keys = nil
    end

    if keys == :sort
      with_unnamed do
        keys = self.keys.sort
      end
    end

    io = dumper_stream(keys, no_options)

    str = ''
    while block = io.read(2048)
      str << block
    end

    str
  end

  def value_peek
    peek = {}
    i = 0
    begin
      through do |k,v|
        peek[k] = v 
        i += 1
        raise "STOP" if i > 10
      end
    rescue
    end
    peek
  end

  def head(times=10)
    stream = dumper_stream
    str = ""
    times.times do |i|
      break if stream.eof?
      str << stream.gets
    end
    str
  end

  def summary

    key = nil
    values = nil
    self.each do |k, v|
      key = k
      values = v
      break
    end

    with_unnamed do
      <<-EOF
Filename = #{Path === filename ? filename.find : (filename || "No filename")}
Key field = #{key_field || "*No key field*"}
Fields = #{fields ? Misc.fingerprint(fields) : "*No field info*"}
Type = #{type}
Serializer = #{serializer.inspect}
Size = #{size}
namespace = #{namespace}
identifiers = #{Misc.fingerprint identifiers}
Example:
  - #{key} -- #{Misc.fingerprint values }
      EOF
    end
  end

  def to_hash
    new = self.dup
    ENTRY_KEYS.each{|entry| new.delete entry}
    new
  end

  def unzip(field = 0, merge = false, sep = ":")
    new = {}
    self.annotate new

    field_pos = self.identify_field field
    new.with_unnamed do
      if merge
        self.through do |key,values|
          field_values = values.delete_at field_pos
          next if field_values.nil?
          zipped = Misc.zip_fields(values)
          field_values.zip(zipped).each do |field_value,rest|
            k = [key,field_value]*sep
            if new.include? k
              new[k] = Misc.zip_fields(Misc.zip_fields(new[k]) << rest)
            else
              new[k] = rest.collect{|v| [v]}
            end
          end
        end
        new.type = :double
      else
        self.through do |key,values|
          field_values = values.delete_at field_pos
          next if field_values.nil?
          zipped = Misc.zip_fields(values)
          field_values.zip(zipped).each do |field_value,rest|
            k = [key,field_value]*sep
            new[k] = rest
          end
        end
        new.type = :list
      end
    end

    if self.key_field and self.fields
      new.key_field = [self.key_field, self.fields[field_pos]] * ":" 
      new_fields = self.fields.dup 
      new_fields.delete_at field_pos
      new.fields = new_fields
    end

    new
  end
end

