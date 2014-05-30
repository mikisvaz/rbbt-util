module SOPT

  class << self
    attr_accessor :command, :summary, :synopsys, :description
  end

  def self.command
    @command ||= File.basename($0)
  end

  def self.summary
    @summary ||= ""
  end

  def self.synopsys
    @synopsys ||= begin
                    "#{command} " <<
                    inputs.collect{|name|
                      "[" << input_format(name, input_types[name] || :string, input_defaults[name], input_shortcuts[name]).sub(/:$/,'') << "]"
                    } * " "
                  end
  end

  def self.description
    @description ||= "Missing"
  end

  def self.input_format(name, type = nil, default = nil, short = nil)
    input_str = (short.nil? or short.empty?) ? "--#{name}" : "-#{short},--#{name}"
    input_str = Log.color(:blue, input_str)
    extra = case type
    when nil
      ""
    when :boolean
      "[=false]" 
    when :tsv, :text
      "=<file|->"
    when :array
      "=<list|file|->"
    else
      "=<#{ type }>"
    end
    extra << " (default '#{default}')" if default != nil
    input_str << Log.color(:green, extra)
  end

  def self.input_doc(inputs, input_types = nil, input_descriptions = nil, input_defaults = nil, input_shortcuts = nil)
    type = description = default = nil
    shortcut = ""
    inputs.collect do |name|

      type = input_types[name] unless input_types.nil?
      description = input_descriptions[name] unless input_descriptions.nil?
      default = input_defaults[name] unless input_defaults.nil?

      name = name.to_s

      case input_shortcuts
      when nil, FalseClass
        shortcut = nil
      when Hash
        shortcut = input_shortcuts[name] 
      when TrueClass
        shortcut = fix_shortcut(name[0], name)
      end

      type = :string if type.nil?
      register(shortcut, name, type, description) unless self.inputs.include? name

      name  = SOPT.input_format(name, type.to_sym, default, shortcut) 
      description 
      Misc.format_definition_list_item(name, description, 80, 31, nil)
    end * "\n"
  end

  def self.doc
    doc = <<-EOF
#{Log.color :magenta}#{command}(1) -- #{summary}
#{"=" * (command.length + summary.length + 7)}#{Log.color :reset}

#{ Log.color :magenta, "## SYNOPSYS"}

#{Log.color :blue, synopsys}

#{ Log.color :magenta, "## DESCRIPTION"}

#{Misc.format_paragraph description}

#{ Log.color :magenta, "## OPTIONS"}

#{input_doc(inputs, input_types, input_descriptions, input_defaults, input_shortcuts)}
    EOF
  end

  def self.doc
    doc = <<-EOF
#{Log.color :magenta}#{command}(1) -- #{summary}
#{"=" * (command.length + summary.length + 7)}#{Log.color :reset}

    EOF

    if synopsys and not synopsys.empty?
      doc << Log.color(:magenta, "## SYNOPSYS") << "\n\n"
      doc << Log.color(:blue, synopsys) << "\n\n"
    end

    if description and not description.empty?
      doc << Log.color(:magenta, "## DESCRIPTION") << "\n\n"
      doc << Misc.format_paragraph(description)
    end

    doc << Log.color(:magenta, "## OPTIONS") << "\n\n"
    doc << input_doc(inputs, input_types, input_descriptions, input_defaults, input_shortcuts)
  end
end
