require 'rbbt/util/resource'
require 'rbbt/util/misc'

class TSV
  ## Make sure we overwrite the methods declared by attr_accessor
  MAIN_ACCESSORS = :data,  :key_field, :fields, :cast
  EXTRA_ACCESSORS = :filename, :identifiers, :namespace, :datadir, :type, :case_insensitive
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

    def fullname
      return self if self =~ /:/ or namespace.nil?
      namespace + ":" << self
    end

    def ==(string)
      return false unless String === string
      return true if self.casecmp(string) == 0
      if Field === string
        return true if self.fullname.casecmp(string.fullname) == 0
      else
        return true if self.fullname.casecmp(string)  == 0
      end
      return true if self.sub(/.*:/,'').casecmp(string) == 0
      return false
    end

    def namespace
      Field.namespace(self) || @namespace
    end

    def matching_namespaces(other)
      return true if namespace.nil?
      return namespace == other.namespace
    end
  end

  #{{{{ Field END

  def identifier_files
    case
    when (identifiers and TSV === identifiers)
      [identifiers]
    when (identifiers and Array === identifiers)
      case
      when (TSV === identifiers.first or identifiers.empty?)
        identifiers
      when
        identifiers.collect{|f| Resource::Path.path(f, nil, namespace)}
      end
    when (identifiers and not Array === identifiers)
      [Resource::Path.path(identifiers, nil, namespace)]
    when filename
      Resource::Path.path(filename, nil, namespace).identifier_files
    else
      []
    end
  end

  def fields_in_namespace(namespace = nil)
    namespace = self.namespace if namespace == nil or TrueClass === namespace
    fields.select{|f| f.namespace.nil? or f.namespace == namespace}
  end

  def key_field
    return nil if @key_field.nil?
    k = @key_field.dup
    k.extend Field
    k
  end

  def fields
    return nil if @fields.nil?
    fds = @fields
    fds.each do |f| f.extend Field end if Array === @fields
    fds.each do |f| f.namespace = namespace end unless namespace.nil?
    NamedArray.name(fds, @fields)
  end

  def all_fields
    return nil if @fields.nil?
    all_fields = @fields.dup
    all_fields.unshift key_field
    all_fields.each do |f| f.extend Field end if Array === @fields
    all_fields.each do |f| f.namespace = namespace end unless namespace.nil?
    NamedArray.name(all_fields, [key_field] +  @fields)
    all_fields
  end

  def all_namespace_fields(namespace = nil)
    namespace = self.namespace if namespace == nil or TrueClass === namespace
    all_fields = self.all_fields
    return nil if all_fields.nil?
    return all_fields if namespace.nil?
    all_fields.select{|f| f.namespace.nil? or f.namespace == namespace}
  end

  def self.identify_field(key, fields, field)
    return field if Integer === field
    if String === field
      field = field.dup
      field.extend Field
    end
    return :key if field.nil? or field == 0 or field.to_sym == :key or field == key
    return nil if fields.nil?
    return fields.collect{|f| f.to_s}.index field if fields.collect{|f| f.to_s}.index field
    return fields.index field 
  end

  def identify_field(field)
    TSV.identify_field(key_field, fields, field)
  end

  def fields=(new_fields)
    new_fields.collect! do |field| 
      if Field === field
        if field !~ /:/ and field.namespace != nil and field.namespace != namespace
          field.namespace + ":" + field.to_s
        else
          field
        end
      else
        field
      end
    end if Array === new_fields
    @fields = new_fields
    @data.fields = new_fields if @data.respond_to? :fields= and @data.write?
  end

  def old_fields=(new_fields)
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

      if Array === value
        value = NamedArray.name value, fields 
      end
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

  def delete(key)
    @data.delete(key)
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
    @data.include? key
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

    str << "#: " << Misc.hash2string(EXTRA_ACCESSORS.collect{|key| [key, self.send(key)]}) << "\n" unless no_options
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

  def value_peek
    peek = {}
    keys[0..10].zip(values[0..10]).each do |k,v| peek[k] = v end
    peek
  end
end
