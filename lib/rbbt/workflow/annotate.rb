module AnnotatedModule

  def self.add_consummable_annotation(target, *annotations)
    if annotations.length == 1 and Hash === annotations.first
      annotations.first.each do |annotation, default|
        target.send(:attr_accessor, annotation)
        target.send(:define_method, "consume_#{annotation}") do
          value = instance_variable_get("@#{annotation}") || default.dup
          instance_variable_set("@#{annotation}", default.dup)
          value
        end
      end
    else
      annotations.each do |annotation|
        target.send(:attr_accessor, annotation)
        target.send(:define_method, "consume_#{annotation}") do
          value = instance_variable_get("@#{annotation}")
          instance_variable_set("@#{annotation}", nil)
        end
      end
    end
  end

end
