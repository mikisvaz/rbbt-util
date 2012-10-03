require 'rbbt/util/chain_methods'
require 'yaml'
module TSV
  extend ChainMethods
  self.chain_prefix = :tsv

  NIL_YAML = "--- \n"

  attr_accessor :unnamed, :serializer_module, :entity_options

  def entity_options
    options = namespace ? {:namespace => namespace, :organism => namespace} : {}
    if @entity_options
      options.merge(@entity_options)
    else
      options
    end
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
    @monitor = value
    res = yield
    @monitor = saved_monitor
    res
  end

  def self.extended(data)
    setup_chains(data)

    if not data.respond_to? :write
      class << data
        attr_accessor :writable

        def close
        end

        def read(force = false)
          @writable = false
          self
        end

        def write(force = false)
          @writable = true
          self
        end

        def write?
          @writable
        end
      end
    end

    if not data.respond_to? :serialized_get
      class << data
        alias serialized_get tsv_clean_get_brackets
        alias serialized_set tsv_clean_set_brackets
      end
    end
  end

  KEY_PREFIX = "__tsv_hash_"

  ENTRIES = []
  ENTRY_KEYS = []

  #{{{ Chained Methods
  def tsv_empty?
    length == 0
  end

  def tsv_get_brackets(key)
    value = serialized_get(key)
    return value if value.nil? or @unnamed or fields.nil?

    case type
    when :double, :list
      NamedArray.setup value, fields, key, entity_options
    when :flat, :single
      value = value.dup if value.frozen?

      value = Misc.prepare_entity(value, fields.first, entity_options)
    end
    value
  end

  def tsv_set_brackets(key,value)
    serialized_set(key, value)
  end

  def tsv_keys
    keys = tsv_clean_keys - ENTRY_KEYS
    return keys if @unnamed or key_field.nil?

    Misc.prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
  end

  def tsv_values
    values = values_at(*keys)
    return values if @unnamed or fields.nil?

    case type
    when :double, :list
      values.each{|value| NamedArray.setup value, fields, nil, entity_options}
    when :flat, :single
      values = values.collect{|v| Misc.prepare_entity(v, fields.first, entity_options)}
    end
      
    values
  end

  def tsv_each
    fields = self.fields

    serializer = self.serializer
    serializer_module = SERIALIZER_ALIAS[serializer] unless serializer.nil?
    tsv_clean_each do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless serializer.nil?

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not fields.nil? 
          case type
          when :double, :list
            NamedArray.setup value, fields, key, entity_options if Array === value
          when :flat, :single
            Misc.prepare_entity(value, fields.first, entity_options)
          end
        end
        key = Misc.prepare_entity(key, key_field, entity_options)
      end

      yield key, value if block_given?
      [key, value]
    end
  end

  def tsv_collect
    serializer = self.serializer
    serializer_module = SERIALIZER_ALIAS[serializer] unless serializer.nil?
    tsv_clean_collect do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless serializer.nil?

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not fields.nil? 
          case type
          when :double, :list
            NamedArray.setup value, fields, key, entity_options if Array === value 
          when :flat, :single
            value = Misc.prepare_entity(value, fields.first, entity_options)
          end
        end
        key = Misc.prepare_entity(key, key_field, entity_options)
      end


      if block_given?
        yield key, value
      else
        [key, value]
      end
    end
  end

  def tsv_size
    keys.length
  end

  def tsv_length
    keys.length
  end

  def tsv_values_at(*keys)
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

  def tsv_sort_by(field = nil, just_keys = false, &block)
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
          keys = Misc.prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
        else
          elems.sort_by{|key, value| key }
        end
      else
        if just_keys
          keys = elems.sort_by{|key, value| value }.collect{|key, value| key}
          keys = Misc.prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
          keys
        else
          elems.sort_by{|key, value| value }.collect{|key, value| [key, self[key]]}
        end
      end
    else
      if just_keys
        elems.sort_by(&block).collect{|key, value| key}
      else
        elems.sort_by(&block).collect{|key, value| [key, self[key]]}
      end
    end
  end

  def tsv_sort(&block)
    collect.sort &block
  end

  # Starts in page 1
  def page(pnum, psize, field = nil, just_keys = false, &block)
    if pnum.to_s =~ /-(.*)/
      reverse = true
      pnum = $1.to_i
    else
      reverse = false
    end

    with_unnamed do
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
  end

  
  def self.entry(*entries)
    entries = entries.collect{|entry| entry.to_s}
    ENTRIES.concat entries
    entries.each do |entry|
      key = KEY_PREFIX + entry
      ENTRY_KEYS << key
      self.module_eval "
attr_accessor :#{entry}

def #{ entry }
  if not defined? @#{entry}
    @#{entry} = (value = self.tsv_clean_get_brackets('#{key}')).nil? ? nil : YAML.load(value)
  end
  @#{entry}
end


if '#{entry}' == 'serializer'

  def #{ entry }=(value)
    @#{entry} = value
    self.tsv_clean_set_brackets '#{key}', value.nil? ? NIL_YAML : value.to_yaml

    return if value.nil?

    self.serializer_module = SERIALIZER_ALIAS[value.to_sym]
    
    if serializer_module.nil?
      class << self
        alias serialized_get tsv_clean_get_brackets
        alias serialized_set tsv_clean_set_brackets
      end

    else
      class << self

        define_method :serialized_get do |key|
          return nil unless self.include? key
          res = tsv_clean_get_brackets(key)
          return res if res.nil?
          self.serializer_module.load(res)
        end

        define_method :serialized_set do |key, value|
          if value.nil?
            tsv_clean_set_brackets key, value
          else
            tsv_clean_set_brackets key, self.serializer_module.dump(value)
          end
        end
      end
    end

  end
else
  def #{ entry }=(value)
    @#{entry} = value
    self.tsv_clean_set_brackets '#{key}', value.nil? ? NIL_YAML : value.to_yaml
  end
end
"
    end
  end

  entry :key_field, 
    :fields, 
    :type,
    :cast,
    :identifiers,
    :namespace,
    :filename,
    :serializer

  def fields
    @fields ||= YAML.load(self.tsv_clean_get_brackets("__tsv_hash_fields") || "--- \n")
    if @fields.nil? or @unnamed
      @fields
    else
      NamedArray.setup @fields, @fields, nil, entity_options
    end
  end

  def self.zip_fields(list, fields = nil)
    return [] if list.nil? || list.empty?
    fields ||= list.fields if list.respond_to? :fields
    zipped = list[0].zip(*list[1..-1])
    zipped = zipped.collect{|v| NamedArray.setup(v, fields)} if fields 
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
        identifiers.collect{|f| Path === f ? f : Path.setup(f, nil, namespace)}
      end
    when identifiers
      [ Path === identifiers ? identifiers : Path.setup(identifiers, nil, namespace) ]
    when Path === filename
      filename.identifier_files
    when filename
      Path.setup(filename).identifier_files
    else
      []
    end
  end

  def options
    options = {}
    ENTRIES.each do |entry|
      options[entry] = self.send(entry)
    end
    IndiferentHash.setup options
  end


  def all_fields
    [key_field] + fields
  end

  def values_to_s(values)
      case
      when (values.nil? and fields.nil?)
        "\n"
      when (values.nil? and not fields.nil?)
        "\t" << ([""] * fields.length) * "\t" << "\n"
      when (not Array === values)
        "\t" << values.to_s << "\n"
      else
        "\t" << values.collect{|v| Array === v ? v * "|" : v} * "\t" << "\n"
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

    str = ""

    str << "#: " << Misc.hash2string((ENTRIES - ["key_field", "fields"]).collect{|key| [key.to_sym, self.send(key)]}) << "\n" unless no_options
    if fields
      str << "#" << key_field << "\t" << fields * "\t" << "\n"
    end

    with_unnamed do
      if keys.nil?
        each do |key, values|
          key = key.to_s if Symbol === key
          str << key.to_s
          str << values_to_s(values)
        end
      else
        keys.zip(values_at(*keys)).each do |key, values|
          key = key.to_s if Symbol === key
          str << key.to_s << values_to_s(values)
        end
      end

    end
    str
  end

  def value_peek
    peek = {}
    keys[0..10].zip(values[0..10]).each do |k,v| peek[k] = v end
    peek
  end

  def to_hash
    new = self.dup
    ENTRY_KEYS.each{|entry| new.delete entry}
    new
  end
end

