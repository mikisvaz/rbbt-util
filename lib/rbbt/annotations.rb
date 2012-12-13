require 'rbbt/util/misc'
require 'rbbt/util/chain_methods'

require 'json'
module Annotated
  attr_accessor :annotation_types
  attr_accessor :context
  attr_accessor :container
  attr_accessor :container_index
  attr_accessor :id

  def self.extended(base)
    base.annotation_types ||= []
  end

  def annotations
    raise "Annotation types is nil for object: #{self.inspect}" if annotation_types.nil?
    annotation_types.collect do |mod|
      mod.annotations
    end.flatten.uniq
  end

  def unmasked_annotations
    raise "Annotation types is nil for object: #{self.inspect}" if annotation_types.nil?
    annotation_types.collect do |mod|
      mod.unmasked_annotations
    end.flatten.uniq
  end

  def info(masked = false)
    hash = {:annotation_types => annotation_types}
    (masked ? unmasked_annotations : annotations).each do |annotation|
      value = self.send(annotation)
      hash[annotation] = value unless value.nil?
    end
    hash
  end

  def id
    @id ||= self.respond_to?(:annotation_id) ? annotation_id : Misc.hash2md5(info.merge(:self => self))
  end

  def self.load(object, info)
    annotation_types = info[:annotation_types] || []
    annotation_types = annotation_types.split("|") if String === annotation_types

    return object if annotation_types.nil? or annotation_types.empty?

    annotated_array = info.delete(:annotated_array) || false

    annotation_types.each do |mod|
      mod = Misc.string2const(mod) if String === mod
      mod.setup(object, *info.values_at(*mod.all_annotations))
    end

    object.id = info[:entity_id] if info.include? :entity_id

    object.extend AnnotatedArray if annotated_array

    object
  end

  def tsv_values(*fields)
    if Array === self and (not AnnotatedArray === self or self.double_array)
      Misc.zip_fields(self.compact.collect{|e| e.tsv_values(fields)})
    else
      fields = fields.flatten
      info = self.info
      info[:annotated_array] = true if AnnotatedArray === self
      values = []

      fields.each do |field|
        values << case
        when Proc === field
          field.call(self)
        when field == "JSON"
          info.to_json
        when field == "annotation_types"
          annotation_types.collect{|t| t.to_s} * "|"
        when field == "literal"
          (Array === self ? "Array:" << self * "|" : self).gsub(/\n|\t/, ' ')
        when info.include?(field.to_sym)
          res = info.delete(field.to_sym)
          Array === res ? "Array:" << res * "|" : res
        when self.respond_to?(field)
          res = self.send(field)
          Array === res ? "Array:"<< res * "|" : res
        end
      end

      values
    end
  end

  def self.resolve_array(entry)
    if entry =~ /^Array:/
      entry["Array:".length..-1].split("|")
    else
      entry
    end
  end

  def self.load_tsv_values(id, values, *fields)
    fields = fields.flatten
    info = {}
    literal_pos = fields.index "literal"

    object = case
             when literal_pos
               values[literal_pos]
             else
               id.dup
             end

    object = resolve_array(object)

    if Array === values.first
      Misc.zip_fields(values).collect do |list|
        fields.each_with_index do |field,i|
          if field == "JSON"
            JSON.parse(list[i]).each do |key, value|
              info[key.to_sym] = value
            end
          else
            info[field.to_sym] = resolve_array(list[i])
          end
        end
      end
    else
      fields.each_with_index do |field,i|
        if field == "JSON"
          JSON.parse(values[i]).each do |key, value|
            info[key.to_sym] = value
          end
        else
          info[field.to_sym] = resolve_array(values[i])
        end
      end
    end

    self.load(object, info)
  end

  def self.json(annotations, literal = false)
    annotations = [annotations] unless Array === annotations
    hash = {}
    annotations.each do |annotation|
      if literal
        hash[annotation.id] = annotation.info.merge(:literal => annotation)
      else
        hash[annotation.id] = annotation.info
      end
    end
    hash.to_json
  end

  def self.tsv(annotations, *fields)
    return nil if annotations.nil?
    fields = case
             when ((fields.compact.empty?) and not annotations.empty?)
               fields = AnnotatedArray === annotations ? annotations.annotations : annotations.compact.first.annotations
               fields << :annotation_types
             when (fields == [:literal] and not annotations.empty?)
               fields << :literal
             when (fields == [:all] and Annotated === annotations)
               fields = [:annotation_types] + annotations.annotations 
               fields << :literal
             when (fields == [:all] and not annotations.empty?)
               raise "Input array must be annotated or its elements must be" if not Annotated === annotations.compact.first and not Array === annotations.compact.first
               raise "Input array must be annotated or its elements must be. No duble arrays of singly annotated entities." if not Annotated === annotations.compact.first and Array === annotations.compact.first
               fields = [:annotation_types] + (Annotated === annotations ? 
                                               annotations.annotations : 
                                               annotations.compact.first.annotations)
               fields << :literal
             when annotations.empty?
               [:annotation_types, :literal]
             else
               fields.flatten
             end

    fields = fields.collect{|f| f.to_s}

    case
    when (Annotated === annotations and not (AnnotatedArray === annotations and annotations.double_array))
      tsv = TSV.setup({}, :key_field => "List", :fields => fields, :type => :list, :unnamed => true)
      annot_id = annotations.id
      tsv[annot_id] = annotations.tsv_values(*fields)
    when Array === annotations 
      tsv = TSV.setup({}, :key_field => "ID", :fields => fields, :type => :list, :unnamed => true)
      annotations.compact.each_with_index do |annotation,i|
        tsv[annotation.id + ":" << i.to_s] = annotation.tsv_values(*fields)
        #tsv[annotation.id] = annotation.tsv_values(*fields)
      end
    else
      raise "Annotations need to be an Array to create TSV"
    end

    tsv
  end

  def self.load_tsv(tsv)
    tsv.with_unnamed do
      annotated_entities = tsv.collect do |id, values|
        Annotated.load_tsv_values(id, values, tsv.fields)
      end

      case tsv.key_field 
      when "List"
        annotated_entities.first
      else
        annotated_entities
      end
    end
  end

  def make_list
    new = [self]
    annotation_types.each do |mod|
      mod.setup(new, *info.values_at(*mod.all_annotations))
    end
    new.context = self.context
    new
  end

  def annotate(object)
    annotation_types.each do |mod|
      mod.setup(object, *info.values_at(*mod.annotations))
    end
    object.context = self.context
    object.container = self.container
    object
  end
end


module Annotation
  def self.extended(base)
    if not base.respond_to? :annotations
      class << base
        attr_accessor :annotations, :inheritance, :all_inheritance, :all_annotations, :masked_annotations

        def unmasked_annotations
          annotations - masked_annotations
        end
        self
      end

      base.annotations = []
      base.masked_annotations = []
      base.inheritance = []
      base.all_annotations = []
      base.all_inheritance = []

      base.module_eval do
        class << self
          alias prev_annotation_extended extended
        end

        def self.extended(object)
          self.send(:prev_annotation_extended, object)
          object.extend Annotated unless Annotated === object
          if not object.annotation_types.include? self
            object.annotation_types.concat self.inheritance 
            object.annotation_types << self
            object.annotation_types.uniq!
          end
        end

        def self.included(base)
          base.inheritance << self
          base.all_inheritance.concat self.all_inheritance if self.respond_to? :all_inheritance
          base.all_inheritance << self
          base.all_inheritance.uniq!
          base.update_annotations
        end
      end

    end
  end

  def update_annotations
    @all_annotations = all_inheritance.inject([]){|acc,mod| acc.concat mod.all_annotations}.concat(@annotations)
  end

  def annotation(*values)
    @annotations.concat values.collect{|v| v.to_sym}
    update_annotations

    module_eval do
      attr_accessor *values
    end
  end

 def setup_info(object, info)
    object.extend self unless self === object
    all_annotations.each do |annotation|
      object.send(annotation.to_s + '=', info[annotation])
    end
  end

  def setup(object, *values)
    return nil if object.nil?

    object.extend self unless self === object

    if Hash === values.last
      values.last.each do |name, value|
        setter = "#{name}="
        next unless object.respond_to? setter
        value = value.split("|") if String === value and value.index "|"
        object.send("#{name}=", value)
      end
    else
      all_annotations.zip(values).each do |name, value|
        value = value.split("|") if String === value and value.index "|"
        object.send("#{name}=", value)
      end
    end

    object
  end
end

module AnnotatedArray
  extend ChainMethods

  self.chain_prefix = :annotated_array

  def double_array
    AnnotatedArray === self.send(:[], 0, true)
  end

  def first
    self[0]
  end

  def last
    self[-1]
  end

  def [](pos, clean = false)
    value = super(pos)
    return nil if value.nil?
    return value if clean
    return value unless String === value or Array === value

    value = value.dup if value.frozen?

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s << "=", self.send(annotation)) end
    end

    value.context         = self.context
    value.container       = self
    value.container_index = pos

    value.extend AnnotatedArray if Array === value and AnnotatedArray === self

    value
  end

  def each
    i = 0
    info = info
    super do |value|
      if String === value or Array === value
        value = value.dup if value.frozen? and not value.nil?

        annotation_types.each do |mod| 
          value.extend mod  unless mod === value
          mod.annotations.each do |annotation| value.send(annotation.to_s << "=", self.send(annotation)) end
        end
        
        value.extend AnnotatedArray if Array === value and AnnotatedArray === self
      
        value.context = self.context
        value.container = self
        value.container_index = i

      end

      i += 1

      yield value
    end
  end

  def collect
    res = []

    if block_given?
      each do |value|
        res << yield(value)
      end
    else
      each do |value|
        res << value
      end
    end

    res
  end

  def select(method = nil, *args)
    res = []
    if method
      res = self.zip(self.send(method, *args)).select{|e,result| result}.collect{|element,r| element}
    else
      each do |value|
        res << value if yield(value)
      end
    end

    annotation_types.each do |mod| 
      res.extend mod  unless mod === res
      mod.annotations.each do |annotation| res.send(annotation.to_s + "=", self.send(annotation)) end
    end

    res.context = self.context
    res.container = self.container

    res.extend AnnotatedArray if AnnotatedArray === self

    res
  end

  def reject
    res = []
    each do |value|
      res << value unless yield(value)
    end

    annotation_types.each do |mod| 
      res.extend mod  unless mod === res
      mod.annotations.each do |annotation| res.send(annotation.to_s + "=", self.send(annotation)) end
    end

    res.context = self.context
    res.container = self.container

    res.extend AnnotatedArray if AnnotatedArray === self

    res
  end

  def subset(list)
    value = (self & list)

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def remove(list)
    value = (self - list)

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def compact
    value = super

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end
 
  def uniq
    value = super

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def flatten
    value = super.dup

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def reverse
    value = super

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def sort_by(&block)
    value = super &block

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.context = self.context
    value.container = self.container

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def sort(&block)
    value = self.collect.sort(&block).collect{|value| value.respond_to?(:clean_annotations) ? value.clean_annotations.dup : value.dup }

    annotation_types.each do |mod| 
      value.extend mod  unless mod === value
      mod.annotations.each do |annotation| value.send(annotation.to_s + "=", self.send(annotation)) end
    end

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def select_by(method)
    case
    when (Symbol === method or String === method)
      method = self.send(method) 
    when Array === method
      method = method.dup
    else
      raise "Unknown format of method: of class #{method.class}"
    end

    value = []
    
    super do |e|
      value << e if method.shift
    end

    value.extend AnnotatedArray if AnnotatedArray === self

    value
  end

  def self.annotate(list)
    list[0].annotate list unless AnnotatedArray === list or list[0].nil? or (not list[0].respond_to? :annotate)
  end

end

