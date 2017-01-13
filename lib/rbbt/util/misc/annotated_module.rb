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


module InputModule
  AnnotatedModule.add_consummable_annotation(self,
    :inputs             => [],
    :required_inputs    => [],
    :input_types        => {},
    :input_descriptions => {},
    :input_defaults     => {},
    :input_options      => {})

  def input(name, type = nil, desc = nil, default = nil, options = nil)
    name = name.to_sym
    type = type.to_sym

    @inputs             = [] if @inputs.nil?
    @input_types        = {} if @input_types.nil?
    @input_descriptions = {} if @input_descriptions.nil?
    @input_defaults     = {} if @input_defaults.nil?
    @input_options      = {} if @input_options.nil?
    @required_inputs    = [] if @required_inputs.nil?

    required = Misc.process_options options, :required if options
    required, default = true, nil if default == :required
    @required_inputs << name  if required

    @inputs                   << name
    @input_types[name]        = type unless type.nil?
    @input_descriptions[name] = desc unless desc.nil?
    @input_defaults[name]     = default unless default.nil?
    @input_options[name]      = options unless options.nil?

  end
end
