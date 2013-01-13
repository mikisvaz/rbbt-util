require 'rbbt/util/misc'
require 'rbbt/annotations/annotated_array'
require 'rbbt/annotations/util'

#{{{ ANNOTATED

module Annotated
  attr_accessor :container, :container_index

  def annotation_values
    @annotation_values ||= {}
  end

  def detach_annotations
    @annotation_values = @annotation_values.dup
    @shared_annotations = false
  end

  def annotation_types

    class << self; self; end.
    included_modules.
    select{|m| 
      Annotation === m
    }
  end

  def annotations
    if @annotations.nil? 
      @annotations = []

      annotation_types.each do |annotation_type|
        @annotations.concat annotation_type.annotations
      end

      @annotations
    else
      @annotations
    end
  end

  def masked_annotations
    if @masked_annotations.nil? 
      @masked_annotations = []

      annotation_types.each do |annotation_type|
        @masked_annotations.concat annotation_type.masked_annotations
      end

      @masked_annotations
    else
      @masked_annotations
    end
  end

  def unmasked_annotations
    @unmasked_annotations ||= annotations - masked_annotations
  end


  def info
    if @info.nil?
      info = annotation_values.dup
      info[:annotation_types] = annotation_types
      info[:annotated_array] = true if AnnotatedArray === self
      @info = info
    end

    @info
  end

  # ToDo This does not make much sense, why not change :id directly
  def id
    @id ||= self.respond_to?(:annotation_id) ? 
      annotation_id : 
      Misc.hash2md5(info.merge(:self => self))
  end

  def annotate(value)

    annotation_types.each do |annotation|
      value.extend annotation
    end

    value.instance_variable_set(:@annotation_values,  annotation_values)

    value.instance_variable_set(:@shared_annotations,  true)
    @shared_annotations = true

    value
  end

  def clean_annotations
    case
    when self.nil?
      nil
    when Array === self
      self.dup.collect{|e| e.respond_to?(:clean_annotations)? e.clean_annotations : e}
    when String === self
      "" << self
    else
      self.dup
    end
  end
end


#{{{ ANNOTATION

module Annotation
  attr_accessor :annotations, :masked_annotations

  def annotations
    @annotations ||= []
  end

  def masked_annotations
    @masked_annotations ||= []
  end

  def unmasked_annotations
    annotations - masked_annotations
  end

  def annotation(*list)

    list.each do |annot|
      annotations << annot.to_sym

      # Getter
      self.send(:define_method, annot.to_s) do 
        annotation_values[annot]
      end

      # Setter
      self.send(:define_method, "#{ annot}=") do |value|
        if @shared_annotations 
          detach_annotations # avoid side effects
          @info = nil
          @id = nil
        end
        annotation_values[annot] = value
      end
    end
  end

  def setup_hash(object, values)
    object.instance_variable_set(:@annotation_values,  values)
    object
  end

  def clean_and_setup_hash(object, hash)
    annotation_values = {}

    hash.each do |key, value|

      begin
        next unless @annotations.include? (key = key.to_sym)
      rescue
        next
      end

      value = value.split("|") if String === value and value.index "|"

      annotation_values[key] = value
    end

    object.instance_variable_set(:@annotation_values,  annotation_values)
    object.instance_variable_set(:@shared_annotations,  true)

    object
  end

  def setup_positional(object, *values)
    annotation_values = {}

    annotations.zip(values).each do |name, value|

      value = value.split("|") if String === value and value.index "|"

      annotation_values[name] = value
    end

    object.instance_variable_set(:@annotation_values,  annotation_values)

    object
  end

  def setup(object, *values)
    return object if object.nil?

    object.extend self
    object.extend AnnotatedArray if Array === object

    if Hash === (hash = values.last)
      clean_and_setup_hash(object, hash)
    else
      setup_positional(object, *values)
    end

    object
  end

  def extended(object)
    object.extend Annotated
  end

  def included(mod)
    mod.instance_variable_set(:@annotations, self.annotations.dup)
    mod.instance_variable_set(:@masked_annotations, self.masked_annotations.dup)
  end
end


if __FILE__ == $0

  module Gene
    extend Annotation
    annotation :format, :organism
  end

  a = %w(1 2 3 4 5 6 6)
  Gene.setup a, "Ensembl Gene ID", "Hsa"

  puts a.reject{|a| a.to_i < 6}.collect{|e| e.format}

  puts 

  puts a.reject{|a| a.to_i < 6}.uniq.collect{|e| e.format}
end
