require 'rbbt/util/chain_methods'

module TSV
  extend ChainMethods
  self.chain_prefix = :tsv

  attr_accessor :unnamed

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
  end

  KEY_PREFIX = "__tsv_hash_"

  ENTRIES = []
  ENTRY_KEYS = []

  def serialized_get(key)
    raise "Uninitialized serializer" if serializer == :type
    serialized_value = tsv_clean_get_brackets(key) 
    SERIALIZER_ALIAS[serializer.to_sym].load(serialized_value) unless serialized_value.nil?
  end

  def serialized_set(key, value)
    raise "Uninitialized serializer" if serializer == :type
    if value.nil?
      tsv_clean_set_brackets(key, nil)
    else
      tsv_clean_set_brackets(key, SERIALIZER_ALIAS[serializer.to_sym].dump(value))
    end
  end

  #{{{ Chained Methods
  def tsv_get_brackets(key)
    value = if serializer.nil?
            tsv_clean_get_brackets(key)
          else
            serialized_get(key)
          end

    NamedArray.setup value, fields if Array === value and not @unnamed
    value
  end

  def tsv_set_brackets(key,value)
    if serializer.nil?
      tsv_clean_set_brackets(key, value)
    else
      serialized_set(key, value)
    end
  end

  def tsv_keys
    tsv_clean_keys - ENTRY_KEYS
  end

  def tsv_values
    values = values_at(*keys)
    values.each{|value| NamedArray.setup value, fields} if Array === values.first and not @unnamed
    values
  end

  def tsv_each
    tsv_clean_each do |key, value|
      next if ENTRY_KEYS.include? key

      value = SERIALIZER_ALIAS[serializer].load(value) unless serializer.nil?
      NamedArray.setup value, fields if Array === value and not @unnamed
      yield key, value if block_given?
      [key, value]
    end
  end

  def tsv_collect
    tsv_clean_collect do |key, value|
      next if ENTRY_KEYS.include? key
      value = SERIALIZER_ALIAS[serializer].load(value) unless serializer.nil? or not String === value 
      NamedArray.setup value, fields if Array === value and not @unnamed
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
          elems.sort_by{|key, value| key }.collect{|key, values| key}
        else
          elems.sort_by{|key, value| key }
        end
      else
        if just_keys
          elems.sort_by{|key, value| value }.collect{|key, value| key}
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
    @#{entry} = YAML.load(self.tsv_clean_get_brackets('#{key}') || nil.to_yaml)
  end
  @#{entry}
end

def #{ entry }=(value)
  @#{entry} = value
  self.tsv_clean_set_brackets '#{key}', value.to_yaml
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
    @fields ||= YAML.load(self.tsv_clean_get_brackets("__tsv_hash_fields") || nil.to_yaml)
    if @fields.nil? or @unnamed
      @fields
    else
      NamedArray.setup @fields, @fields
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
      when
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
      keys = self.keys.sort
    end

    str = ""

    str << "#: " << Misc.hash2string(ENTRIES.collect{|key| [key.to_sym, self.send(key)]}) << "\n" unless no_options
    if fields
      str << "#" << key_field << "\t" << fields * "\t" << "\n"
    end

    saved_unnamed = @unnamed
    @unnamed = false
    if keys.nil?
      each do |key, values|
        key = key.to_s if Symbol === key
        str << key.dup 
        str << values_to_s(values)
      end
    else
      keys.zip(values_at(*keys)).each do |key, values|
        key = key.to_s if Symbol === key
        str << key.dup << values_to_s(values)
      end
    end

    @unnamed = saved_unnamed
    str
  end

  def value_peek
    peek = {}
    keys[0..10].zip(values[0..10]).each do |k,v| peek[k] = v end
    peek
  end
end

