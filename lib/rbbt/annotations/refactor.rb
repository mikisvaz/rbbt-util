module Annotation
  def self.extended(base)
    meta = class << base; self; end

    base.extend MetaExtension

    meta.define_method(:annotations) do
      self.instance_variable_get(:@extension_attrs)
    end

    meta.define_method(:annotation) do |*args|
      base.extension_attr(*args)
    end

    base.define_method(:id) do
      self.extended_digest
    end

    base.define_method(:annotation_types) do
      self.extension_types
    end

    base.define_method(:info) do
      self.extension_attr_hash.merge(:annotation_types => annotation_types, :annotated_array => (Array === self))
    end

    base.alias_method(:clean_annotations, :purge)
  end
end

Annotated = MetaExtension

module MetaExtension::ExtendedObject
  alias make_list make_array
end
AnnotatedArray = ExtendedArray

module MetaExtension
  def self.load_entity(obj, info)
    self.setup(obj, info[:annotation_types], info)
  end
end

