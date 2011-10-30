require 'rbbt/util/chain_methods'
require 'json'
module Annotated
  attr_accessor :annotation_types
  attr_accessor :context
  attr_accessor :container

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
    annotation_types.each do |mod|
      mod = Misc.string2const(mod) if String === mod
      mod.setup_info(object, info)
    end

    object
  end

  def tsv_values(*fields)
    fields = fields.flatten
    info = self.info
    values = []
    fields.each do |field|
      values << case
      when field == "JSON"
        info.to_json
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

  def self.load_tsv_values(id, values, *fields)
    fields = fields.flatten
    info = {}
    literal_pos = fields.index "literal"

    object = if literal_pos.nil?
               id
             else
               v = values[literal_pos]
               v = v.first if Array === v
               v
             end

    fields.each_with_index do |field,i|
      if field == "JSON"
        JSON.parse(values[i]).each do |key, value|
          info[key.to_sym] = value
        end
      else
        info[field.to_sym] = values[i]
      end
    end

    self.load(object, info)
  end

  def self.tsv(annotations, *fields)
    fields = case
             when ((fields.compact.empty?) and not annotations.empty?)
               fields = annotations.first.annotations
               fields << :annotation_types
             when (fields == [:literal] and not annotations.empty?)
               fields = annotations.first.annotations
               fields << :literal
             when (fields == [:all] and not annotations.empty?)
               fields = [:annotation_types] + annotations.first.annotations
               fields << :literal
             else
               fields.flatten
             end
    fields = fields.collect{|f| f.to_s}

    tsv = TSV.setup({}, :key_field => "ID", :fields => fields, :type => :list )

    annotations.each do |annotation|
      tsv[annotation.id] = annotation.tsv_values(fields)
    end

    tsv
  end

  def self.load_tsv(tsv)
    tsv.collect do |id, values|
      Annotated.load_tsv_values(id, values, tsv.fields)
    end
  end

  def make_list
    new = [self]
    annotation_types.each do |mod|
      mod.setup(new, *info.values_at(*mod.annotations))
    end
    new.context = self.context
    new
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
          object.extend Annotated
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
    @all_annotations = all_inheritance.inject([]){|acc,mod| acc.concat mod.annotations}.concat(@annotations)
  end

  def annotation(*values)
    @annotations.concat values.collect{|v| v.to_sym}
    update_annotations

    module_eval do
      attr_accessor *values
    end
  end

 def setup_info(object, info)
    object.extend self
    all_annotations.each do |annotation|
      object.send(annotation.to_s + '=', info[annotation])
    end
  end

  def setup(object, *values)
    object.extend self

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

  def annotated_array_get_brackets(pos)
    value = annotated_array_clean_get_brackets(pos)
    annotation_types.each do |mod|
      mod.setup(value, *info.values_at(*mod.annotations))
    end
    value.context = self.context
    value.container = self
    value
  end

  def annotated_array_each
    annotated_array_clean_each do |value|
      annotation_types.each do |mod|
        mod.setup(value, info)
      end
      value.context = self.context
      value.container = self
      yield value
    end
  end

  def annotated_array_collect
    res = []
    annotated_array_each do |value|
      res << yield(value)
    end

    annotation_types.each do |mod|
      mod.setup(res, *info.values_at(*mod.annotations))
    end
    res.context = self.context
    res.container = self.container

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

    #annotation_types.each do |mod|
    #  mod.setup(value, *info.values_at(*mod.annotations))
    #end
    #value.context = self.context
    #value.container = self.container
    value
  end
end
