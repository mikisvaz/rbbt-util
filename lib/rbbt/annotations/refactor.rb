module Annotation
  module AnnotatedObject
    alias id annotation_id
    alias clean_annotations purge
    alias annotation_values annotation_hash
    def info
      annotation_hash.merge(:annotation_types => annotation_types, :annotated_array => (Array === self))
    end
  end
end

Annotated = Annotation::AnnotatedObject

module Annotation::AnnotatedObject
  alias make_list make_array
end

module Annotation
  def self.load_entity(obj, info)
    self.setup(obj, info[:annotation_types], info)
  end
end

