#require 'rbbt/util/chain_methods'
require 'rbbt/util/misc'

module NamedArray
  #extend ChainMethods
  #self.chain_prefix = :named_array

  attr_accessor :fields
  attr_accessor :key
  attr_accessor :entity_options
  attr_accessor :entity_templates

  def entity_templates
    @entity_templates ||= {}
  end

  def self.setup(array, fields, key = nil, entity_options = nil, entity_templates = nil)
    array.extend NamedArray unless NamedArray === array
    array.fields = fields
    array.key = key
    array.entity_options = entity_options unless entity_options.nil?
    array.entity_templates = entity_templates unless entity_templates.nil?
    array
  end

  def prepare_entity(entity, field, options = {})
    return entity if entity.nil?
    return entity unless defined? Entity
    template = entity_templates[field]
    entity_templates ||= {}
    if template
      entity = template.annotate(entity.frozen? ? entity.dup : entity)
      entity.extend AnnotatedArray if Array === entity
      entity
    else
      if entity_templates.include? field
        entity
      else
        template = Misc.prepare_entity("ENTITY_TEMPLATE", field, options)
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

  def merge(array)
    double = Array === array.first 
    new = self.dup
    (0..length - 1).each do |i|
      if double
        new[i] = new[i] + array[i]
      else
        new[i] << array[i]
      end
    end
    new
  end

  def positions(fields)
    if Array ==  fields
      fields.collect{|field|
        Misc.field_position(@fields, field)
      }
    else
      Misc.field_position(@fields, fields)
    end
  end

  #def named_array_get_brackets(key)
  #  pos = Misc.field_position(fields, key)
  #  elem = named_array_clean_get_brackets(pos)

  #  return elem if @fields.nil? or @fields.empty?

  #  field = NamedArray === @fields ? @fields.named_array_clean_get_brackets(pos) : @fields[pos]
  #  elem = prepare_entity(elem, field, entity_options)
  #  elem
  #end

  def [](key, clean = false)
    pos = Misc.field_position(fields, key)
    elem = super(pos)
    return elem if clean

    return elem if @fields.nil? or @fields.empty?

    field = NamedArray === @fields ? @fields[pos, true] : @fields[pos]
    elem = prepare_entity(elem, field, entity_options)
    elem
  end

  #def named_array_each(&block)
  #  if defined?(Entity) and not @fields.nil? and not @fields.empty?
  #    @fields.zip(self).each do |field,elem|
  #      elem = prepare_entity(elem, field, entity_options)
  #      yield(elem)
  #      elem
  #    end
  #  else
  #    named_array_clean_each &block
  #  end
  #end

  def each(&block)
    if defined?(Entity) and not @fields.nil? and not @fields.empty?
      @fields.zip(self).each do |field,elem|
        elem = prepare_entity(elem, field, entity_options)
        yield(elem)
        elem
      end
    else
      super &block
    end

  end

  #def named_array_collect
  #  res = []

  #  each do |elem|
  #    if block_given?
  #      res << yield(elem)
  #    else
  #      res << elem
  #    end
  #  end

  #  res
  #end


  def collect
    res = []

    each do |elem|
      if block_given?
        res << yield(elem)
      else
        res << elem
      end
    end

    res
  end

  #def named_array_set_brackets(key,value)
  #  named_array_clean_set_brackets(Misc.field_position(fields, key), value)
  #end
  
  def []=(key, value)
    super(Misc.field_position(fields, key), value)
  end

  #def named_array_values_at(*keys)
  #  keys = keys.collect{|k| Misc.field_position(fields, k, true) }
  #  keys.collect{|k|
  #    named_array_get_brackets(k) unless k.nil?
  #  }
  #end



  def values_at(*keys)
    keys = keys.collect{|k| Misc.field_position(fields, k, true) }
    keys.collect{|k|
      self[k] unless k.nil?
    }
  end

  def zip_fields
    return [] if self.empty?
    zipped = Misc.zip_fields(self)
    zipped = zipped.collect{|v| NamedArray.setup(v, fields)}
    zipped 
  end

  def detach(file)
    file_fields = file.fields.collect{|field| field.fullname}
    detached_fields = []
    self.fields.each_with_index{|field,i| detached_fields << i if file_fields.include? field.fullname}
    fields = self.fields.values_at *detached_fields
    values = self.values_at *detached_fields
    values = NamedArray.name(values, fields)
    values.zip_fields
  end

  def report
    fields.zip(self).collect do |field,value|
      "#{ field }: #{ Array === value ? value * "|" : value }"
    end * "\n"
  end

end

