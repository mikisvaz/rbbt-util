module AnnotatedModule
  def self.extended(base)
    if not base.respond_to? :inputs
      class << base
        attr_accessor :description, :inputs, :input_types, :input_descriptions, :input_defaults, :result_description, :helpers

        def description
          i = @description; @description = ""; i
        end

        def inputs
          i = @inputs; @inputs = []; i
        end

        def input_types
          i = @input_types; @input_types = {}; i
        end

        def input_descriptions
          i = @input_descriptions; @input_descriptions = {}; i
        end

        def input_defaults
          i = @input_defaults; @input_defaults = {}; i
        end

        def description
          i = @description; @description = ""; i
        end

        def result_description
          i = @result_description; @result_description = nil; i
        end
      end

      base.description = ""
      base.inputs = []
      base.input_types = {}
      base.input_descriptions = {}
      base.input_defaults = {}
      base.helpers = {}

    end
  end

  def helper(name, &block)
    @helpers[name] = block
  end

  def returns(text)
    @result_description = text
  end

  def desc(description)
    @description = description
  end

  def dep(*dependencies, &block)
    dependencies << block if block_given?
    @dependencies.concat dependencies
  end

  def input(name, type = nil, desc = nil, default = nil)
    name = name.to_sym
    type = type.to_sym
    @inputs << name
    @input_types[name] = type unless type.nil?
    @input_descriptions[name] = desc unless desc.nil?
    @input_defaults[name] = default unless default.nil?
  end
end


