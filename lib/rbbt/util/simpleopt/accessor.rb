
module SOPT
  class << self
    attr_accessor :inputs, :input_shortcuts, :input_types, :input_descriptions, :input_defaults
  end

  def self.all
    @all ||= {}
  end

  def self.shortcuts
    @shortcuts ||= {}
  end
  
  def self.inputs 
    @inputs ||= []
  end

  def self.input_shortcuts 
    @input_shortcuts ||= {}
  end

  def self.input_types 
    @input_types ||= {}
  end

  def self.input_descriptions 
    @input_descriptions ||= {}
  end

  def self.input_defaults 
    @input_defaults ||= {}
  end

  def self.reset
    @shortcuts = {}
    @all = {}
  end

  def self.delete_inputs(inputs)
    inputs.each do |input|
      input = input.to_s
      self.shortcuts.delete self.input_shortcuts.delete(input)
      self.inputs.delete input
      self.input_types.delete input
      self.input_defaults.delete input
      self.input_descriptions.delete input
    end
  end
end
