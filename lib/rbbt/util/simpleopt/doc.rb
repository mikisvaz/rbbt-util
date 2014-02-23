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
    input_str = (short.nil? or short.empty?) ? Log.color(:blue,"--#{name}") : Log.color(:blue, "-#{short}") << ", " << Log.color(:blue, "--#{name}")
    input_str = Log.color(:blue, input_str)
    input_str << case type
    when nil
      "#{default != nil ? " (default '#{default}')" : ""}:"
    when :boolean
      "[=false]#{default != nil ? " (default '#{default}')" : ""}:"
    when :tsv, :text
      "=<filename.#{type}|->#{default != nil ? " (default '#{default}')" : ""}; Use '-' for STDIN:"
    when :array
      "=<string[,string]*|filename.list|->#{default != nil ? " (default '#{default}')" : ""}; Use '-' for STDIN:"
    else
      "=<#{ type }>#{default != nil ? " (default '#{default}')" : ""}:"
    end
  end

  def self.input_doc(inputs, input_types = nil, input_descriptions = nil, input_defaults = nil, input_shortcuts = nil)
    type = description = default = nil
    shortcut = ""
    inputs.collect do |name|
      name = name.to_s

      type = input_types[name] unless input_types.nil?
      description = input_descriptions[name] unless input_descriptions.nil?
      default = input_defaults[name] unless input_defaults.nil?

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

      str  = "  * " << SOPT.input_format(name, type.to_sym, default, shortcut) 
      str << "\n     " << description << "\n" if description and not description.empty?
      str
    end * "\n"
  end

  def self.doc
    doc = <<-EOF
#{Log.color :magenta}
#{command}(1) -- #{summary}
#{"=" * (command.length + summary.length + 7)}
#{Log.color :reset}

#{ Log.color :magenta, "## SYNOPSYS"}

#{synopsys}

#{ Log.color :magenta, "## DESCRIPTION"}

#{description}

#{ Log.color :magenta, "## OPTIONS"}

#{input_doc(inputs, input_types, input_descriptions, input_defaults, input_shortcuts)}
    EOF
  end
end
