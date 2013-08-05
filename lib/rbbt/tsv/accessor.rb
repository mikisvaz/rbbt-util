#require 'rbbt/util/chain_methods'
require 'yaml'
module TSV
  #extend ChainMethods
  #self.chain_prefix = :tsv

  TSV_SERIALIZER = YAML
  SERIALIZED_NIL = TSV_SERIALIZER.dump nil

  attr_accessor :unnamed, :serializer_module, :entity_options, :entity_templates

  def annotate(tsv)
    TSV.setup(tsv, :key_field => key_field, :fields => fields, :namespace => namespace, :entity_options => entity_options, :type => type, :filename => filename, :identifiers => identifiers)
  end

  def entity_options
    if @entity_options.nil?
      @entity_options = namespace ? {:namespace => namespace, :organism => namespace} : {}
      @entity_templates = nil
    end
    @entity_options
  end

  def entity_options=(options)
    @entity_options = options
    @entity_templates = nil
  end


  def entity_templates
    @entity_templates ||= {}
  end

  def prepare_entity(entity, field, options = {})
    return entity if entity.nil?
    return entity unless defined? Entity
    entity = entity if options.delete :dup_array
    entity_templates
    if (template = entity_templates[field])
      entity = template.annotate(entity.frozen? ? entity.dup : entity)
      entity.extend AnnotatedArray if Array === entity
      entity
    else
      if entity_templates.include? field
        entity
      else
        template = Misc.prepare_entity("TEMPLATE", field, options)
        if Annotated === template
          entity_templates[field] = template
          entity = template.annotate(entity.frozen? ? entity.dup : entity)
          entity.extend AnnotatedArray if Array === entity
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
    @monitor = value
    res = yield
    @monitor = saved_monitor
    res
  end

  def self.extended(data)
    #setup_chains(data)

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
      #class << data
      #  alias serialized_get []
      #  alias serialized_set []=
      #end
    end
  end

  KEY_PREFIX = "__tsv_hash_"

  ENTRIES = []
  ENTRY_KEYS = []

  #{{{ Chained Methods
  def empty?
    length == 0
  end

  def [](key, clean = false)
    value = (self.respond_to?(:serialized_get) and not clean) ? serialized_get(key) : super(key)
    return value if value.nil? or @unnamed or clean == :entry_key or fields.nil?

    case type
    when :double, :list
      setup_array value, fields, key, entity_options, entity_templates
    when :flat, :single
      value = value.dup if value.frozen?

      value = prepare_entity(value, fields.first, entity_options)
    end
    value
  end

  def []=(key, value, clean = false)
    return super(key, value) if clean or not self.respond_to?(:serialized_set)
    serialized_set(key, value)
  end

  def keys
    keys = super - ENTRY_KEYS
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

    serializer = self.serializer
    serializer_module = SERIALIZER_ALIAS[serializer] unless serializer.nil?
    super do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless serializer.nil? or FalseClass === serializer

      # Annotated with Entity and NamedArray
      if not @unnamed
        if not fields.nil? 
          case type
          when :double, :list
            setup_array value, fields, key, entity_options, entity_templates if Array === value
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
    serializer = self.serializer
    serializer_module = SERIALIZER_ALIAS[serializer] unless serializer.nil?
    super do |key, value|
      next if ENTRY_KEYS.include? key

      # TODO Update this to be more efficient
      value = serializer_module.load(value) unless serializer.nil?

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
    keys.length
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
          elems << [key, fields]
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
          case
          when ((a_value.nil? or (a_value.respond_to?(:empty?) and a_value.empty?)) and (b_value.nil? or (b_value.respond_to?(:empty?) and b_value.empty?)))
            0
          when (a_value.nil? or (a_value.respond_to?(:empty?) and a_value.empty?))
            -1
          when (b_value.nil? or (b_value.respond_to?(:empty?) and b_value.empty?))
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
          #keys = elems.sort_by{|key, value| value }.collect{|key, value| key}
          keys = sorted.collect{|key, value| key}
          keys = prepare_entity(keys, key_field, entity_options.merge(:dup_array => true))
          keys
        else
          sorted.collect{|key, value| [key, self[key]]}
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


  def self.entry(*entries)
    entries = entries.collect{|entry| entry.to_s}
    ENTRIES.concat entries
    entries.each do |entry|
      key = KEY_PREFIX + entry
      ENTRY_KEYS << key
      line = __LINE__; self.module_eval "
attr_accessor :#{entry}

def #{ entry }
  if not defined? @#{entry}
    # @#{entry} = (value = self.clean_get_brackets('#{key}')).nil? ? nil : TSV_SERIALIZER.load(value)
    @#{entry} = (value = self.send(:[], '#{key}', :entry_key)).nil? ? nil : TSV_SERIALIZER.load(value)
  end
  @#{entry}
end


if '#{entry}' == 'serializer'

  def #{ entry }=(value)
    @#{entry} = value
    #self.tsv_clean_set_brackets '#{key}', value.nil? ? SERIALIZED_NIL : value.to_yaml
    self.send(:[]=, '#{key}', value.nil? ? SERIALIZED_NIL : value.to_yaml, true)

    return if value.nil?

    self.serializer_module = SERIALIZER_ALIAS[value.to_sym]

    if serializer_module.nil?
      #class << self
      #  alias serialized_get tsv_clean_get_brackets
      #  alias serialized_set tsv_clean_set_brackets
      #end

    else
      class << self

        define_method :serialized_get do |key|
          return nil unless self.include? key
          res = self.send(:[], key, true)
          return res if res.nil?
          self.serializer_module.load(res)
        end

        define_method :serialized_set do |key, value|
          if value.nil?
            self.send(:[]=, key, value, true)
            #tsv_clean_set_brackets key, value
          else
            self.send(:[]=, key, self.serializer_module.dump(value), true)
            #tsv_clean_set_brackets key, self.serializer_module.dump(value)
          end
        end
      end
    end

  end
else
  def #{ entry }=(value)
    @#{entry} = value
    self.send(:[]=, '#{key}', value.nil? ? SERIALIZED_NIL : value.to_yaml, true)
    #self.tsv_clean_set_brackets '#{key}', value.nil? ? SERIALIZED_NIL : value.to_yaml
  end
end
  ", __FILE__, line
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

  def fields
    #@fields ||= TSV_SERIALIZER.load(self.tsv_clean_get_brackets("__tsv_hash_fields") || SERIALIZED_NIL)
    @fields ||= TSV_SERIALIZER.load(self.send(:[], "__tsv_hash_fields", :entry_key) || SERIALIZED_NIL)
    if true or @fields.nil? or @unnamed
      @fields
    else
      @named_fields ||= NamedArray.setup @fields, @fields, nil, entity_options, entity_templates
    end
  end

  def namespace=(value)
    #self.tsv_clean_set_brackets "__tsv_hash_namespace", value.nil? ? SERIALIZED_NIL : value.to_yaml
    self.send(:[]=, "__tsv_hash_namespace", value.nil? ? SERIALIZED_NIL : value.to_yaml, true)
    @namespace = value
    @entity_options = nil
  end

  def fields=(value)
    #self.tsv_clean_set_brackets "__tsv_hash_fields", value.nil? ? SERIALIZED_NIL : value.to_yaml
    self.send(:[]=, "__tsv_hash_fields", value.nil? ? SERIALIZED_NIL : value.to_yaml, true)
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
        identifiers.collect{|f| Path === f ? f : Path.setup(f, nil, namespace)}
      end
    when identifiers
      [ Path === identifiers ? identifiers : Path.setup(identifiers, nil, namespace) ]
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

  def summary
    <<-EOF
Key field = #{key_field}
Fields = #{fields * ", "}
Type = #{type}
Example:
  - #{key = keys.first}: #{self[key].inspect}

    EOF
  end

  def to_hash
    new = self.dup
    ENTRY_KEYS.each{|entry| new.delete entry}
    new
  end
end

