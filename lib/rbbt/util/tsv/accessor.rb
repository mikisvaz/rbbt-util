require 'rbbt/util/misc'

class TSV
  ## Make sure we overwrite the methods declared by attr_accessor
  MAIN_ACCESSORS = :data,  :key_field, :fields, :cast
  EXTRA_ACCESSORS = :filename, :identifiers, :datadir, :type, :namespace, :case_insensitive
  attr_accessor *(MAIN_ACCESSORS + EXTRA_ACCESSORS)

  def self.zip_fields(list, fields = nil)
    return [] if list.nil? || list.empty?
    fields ||= list.fields if list.respond_to? :fields
    zipped = list[0].zip(*list[1..-1])
    zipped = zipped.collect{|v| NamedArray.name(v, fields)} if fields 
    zipped 
  end


  module Field
    attr_accessor :namespace

    def self.field(field, namespace = nil)
      field.extend Field
      field.namespace = namespace
      field
    end

    def self.namespace(string)
      return nil unless string.match(/(.+):/)
      namespace_str = $1
      return nil if namespace_str.nil? or namespace_str.empty?
      namespace_str
    end

    def ==(string)
      return false unless String === string
      self.sub(/.*:/,'').casecmp(string.sub(/.*:/,'')) == 0
    end

    def namespace
      Field.namespace(self) || @namespace
    end

    def matching_namespaces(other)
      return true if namespace.nil?
      return namespace == other.namespace
    end
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
        identifiers.collect{|f| Path.path(f, datadir, namespace)}
      end
    when (identifiers and not Array === identifiers)
      [Path.path(identifiers, datadir, namespace)]
    when (not namespace.nil? and Misc.string2const(namespace) and Misc.string2const(namespace).respond_to? :identifier_files)
      Misc.string2const(namespace).identifier_files
    when filename
      Path.path(filename, datadir, namespace).identifier_files
    else
      []
    end
  end

  def fields
    return nil if @fields.nil?
    fields = @fields
    fields.each do |f| f.extend Field end if Array === fields
    fields.each do |f| f.namespace = namespace end unless namespace.nil?
    NamedArray.name(fields, @fields)
  end

  def all_fields
    return nil if @fields.nil?
    fields = @fields.dup
    fields.unshift key_field
    fields.each do |f| f.extend Field end if Array === fields
    NamedArray.name(fields, [key_field] +  @fields)
    fields
  end

  def self.identify_field(key, fields, field)
    return field if Integer === field
    return :key if field.nil? or field == 0 or field.to_sym == :key or key == field 
    return nil if fields.nil?
    return fields.collect{|f| f.to_s}.index field if fields.collect{|f| f.to_s}.index field
    return fields.index field 
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end


  def fields=(new_fields)
    @fields = new_fields
    @data.fields = new_fields if @data.respond_to? :fields=
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
        yield key, value
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

  def include?(key)
    data.include? key
  end

  def to_s(keys = nil)
    str = ""

    str << "#: " << Misc.hash2string(EXTRA_ACCESSORS.collect{|key| [key, self.send(key)]}) << "\n"
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
end
