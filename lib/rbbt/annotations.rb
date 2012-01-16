require 'rbbt/util/misc'
require 'rbbt/util/chain_methods'

require 'json'
module Annotated
  attr_accessor :annotation_types
  attr_accessor :context
  attr_accessor :container
  attr_accessor :container_index

  def self.extended(base)
    base.annotation_types ||= []
  end

  def annotations
    annotation_types.collect do |mod|
      mod.annotations
    end.flatten.uniq
  end

  def info
    hash = {:annotation_types => annotation_types}
    annotations.each do |annotation|
      value = self.send(annotation)
      hash[annotation] = value unless value.nil?
    end
    hash
  end

  def id
    Misc.hash2md5 info.merge :self => self
  end

  def self.load(object, info)
    annotation_types = info[:annotation_types]
    annotation_types = annotation_types.split("+") if String === annotation_types

    annotation_types.each do |mod|
      mod = Misc.string2const(mod) if String === mod
      mod.setup(object, *info.values_at(*mod.all_annotations))
    end

    object
  end

  def tsv_values(*fields)
    if Array === self and (not AnnotatedArray === self or self.double_array)
      Misc.zip_fields(self.compact.collect{|e| e.tsv_values(fields)})
    else
      fields = fields.flatten
      info = self.info
      values = []

      fields.each do |field|
        values << case
        when Proc === field
          field.call(self)
        when field == "JSON"
          info.to_json
        when field == "annotation_types"
          annotation_types.collect{|t| t.to_s} * "+"
        when field == "literal array"
          (self * "|").gsub(/\n|\t/, ' ')
        when field == "literal"
          self.gsub(/\n|\t/, ' ')
        when info.include?(field.to_sym)
          info.delete(field.to_sym)
        when self.respond_to?(field)
          self.send(field)
        end
      end

      values
    end
  end

  def self.load_tsv_values(id, values, *fields)
    fields = fields.flatten
    info = {}
    literal_pos = fields.index "literal"
    literal_array_pos = fields.index "literal array"

    object = case
             when literal_pos
               values[literal_pos]
             when literal_array_pos
               values[literal_array_pos].split("|").extend AnnotatedArray
             else
               id.dup
             end

    if Array === values
      Misc.zip_fields(values).collect do |list|
        fields.each_with_index do |field,i|
          if field == "JSON"
            JSON.parse(list[i]).each do |key, value|
              info[key.to_sym] = value
            end
          else
            info[field.to_sym] = list[i]
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
          info[field.to_sym] = values[i]
        end
      end
    end

    self.load(object, info)
  end

  def self.tsv(annotations, *fields)
    return nil if annotations.nil?
    fields = case
             when ((fields.compact.empty?) and not annotations.empty?)
               fields = AnnotatedArray === annotations ? annotations.annotations : annotations.first.annotations
               fields << :annotation_types
             when (fields == [:literal] and not annotations.empty?)
               fields << :literal
             when (fields == [:all] and not annotations.empty?)
               fields = [:annotation_types] + (Annotated === annotations ? annotations.annotations : annotations.first.annotations)
               fields << :literal
             else
               fields.flatten
             end

    fields = fields.collect{|f| f.to_s}

    fields = fields.collect{|f| ((f == "literal" and AnnotatedArray === annotations) ? "literal array" : f)}

    case
    when (Annotated === annotations and not annotations.double_array)
      tsv = TSV.setup({}, :key_field => "Single", :fields => fields, :type => :list, :unnamed => true)
      tsv[annotations.id] = annotations.tsv_values(*fields)
    when Array === annotations 
      tsv = TSV.setup({}, :key_field => "ID", :fields => fields, :type => :list, :unnamed => true)
      annotations.compact.each do |annotation|
        tsv[annotation.id] = annotation.tsv_values(*fields)
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

      if tsv.key_field == "Single"
        annotated_entities.first
      else
        annotated_entities[0].annotate annotated_entities unless annotated_entities.empty?
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
        attr_accessor :annotations, :inheritance, :all_inheritance, :all_annotations
        self
      end

      base.annotations = []
      base.inheritance = []
      base.all_annotations = []
      base.all_inheritance = []

      base.module_eval do
        class << self
          alias prev_annotation_extended extended
        end

        def self.extended(object)
          self.send(:prev_annotation_extended, object)
            object.extend Annotated unless Annotated == object
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
    object.extend self unless self === object

    inputs = Misc.positional2hash(all_annotations, *values)
    inputs.each do |name, value|
      object.send(name.to_s + '=', value)
    end

    object
  end

end

module AnnotatedArray
  extend ChainMethods
  self.chain_prefix = :annotated_array

  def double_array
    AnnotatedArray === self.annotated_array_clean_get_brackets(0)
  end

  def annotated_array_first
    self[0]
  end

  def annotated_array_last
    self[-1]
  end

  def annotated_array_get_brackets(pos)
    value = annotated_array_clean_get_brackets(pos)
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.all_annotations))
    end
    value.context = self.context
    value.container = self
    value.container_index = pos
    value
  end

  def annotated_array_each
    i = 0
    annotated_array_clean_each do |value|
      value = value.dup if value.frozen?
      annotation_types.each do |mod|
        mod.setup(value, info)
      end
      value.context = self.context
      value.container = self
      value.container_index = i
      i += 1
      yield value
    end
  end

  def annotated_array_collect
    res = []

    annotated_array_each do |value|
      res << yield(value)
    end

    res
  end

  def annotated_array_select
    res = []
    annotated_array_each do |value|
      res << value if yield(value)
    end

    annotation_types.each do |mod|
      mod.setup(res, *info.values_at(*mod.annotations))
    end
    res.context = self.context
    res.container = self.container

    res
  end

  def annotated_array_reject
    res = []
    annotated_array_each do |value|
      res << value unless yield(value)
    end

    annotation_types.each do |mod|
      mod.setup(res, *info.values_at(*mod.annotations))
    end
    res.context = self.context
    res.container = self.container

    res
  end

  def annotated_array_subset(list)
    value = (self & list)
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.all_annotations))
    end
    value.context = self.context
    value.container = self.container
    value
  end

  def annotated_array_remove(list)
    value = (self - list)
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end
    value.context = self.context
    value.container = self.container
    value
  end

  def annotated_array_compact
    value = self.annotated_array_clean_compact

    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end

    value.context = self.context
    value.container = self.container
    value
  end
 
  def annotated_array_uniq
    value = self.annotated_array_clean_uniq

    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end

    value.context = self.context
    value.container = self.container
    value
  end
  def annotated_array_flatten
    value = self.annotated_array_clean_flatten.dup

    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end

    value.context = self.context
    value.container = self.container
    value
  end

  def annotated_array_reverse
    value = self.annotated_array_clean_reverse
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end
    value.context = self.context
    value.container = self.container
    value
  end


  def annotated_array_sort_by(&block)
    value = self.annotated_array_clean_sort_by &block
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end
    value.context = self.context
    value.container = self.container
    value
  end

  def annotated_array_sort
    value = self.annotated_array_clean_sort

    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end
    value.context = self.context
    value.container = self.container
    value
  end

  def annotated_array_select_by(method)
    case
    when (Symbol === method or String === method)
      method = self.send(method) 
    when Array === method
      method = method.dup
    else
      raise "Unknown format of method: of class #{method.class}"
    end

    value = []
    
    self.annotated_array_clean_each do |e|
      value << e if method.shift
    end

    value
  end
end
