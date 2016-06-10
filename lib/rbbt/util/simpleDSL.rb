# This class helps designing DSL in ruby based on method_missing. Class
# is initialize with a block of code or a file with the code, and it is
# given a method to be invoked instead of method missing. This class
# deals simply with making the method_missing alias and removing it and
# executing the block of file with code.
module SimpleDSL

  class ConfigFileMissingError < StandardError; end
  class NoRuby2Ruby < StandardError; end

  private

  def hook_method(method = nil)
    method ||= :DSL_action 
    @@restore_name = ("restore_DSL_" + method.to_s).to_sym
    @@method_name = method.to_sym
    
    class << self
      @restore_stack ||= []
      @restore_stack << @@restore_name
      alias_method(@@restore_name, :method_missing)
      alias_method(:method_missing, @@method_name)
    end
  end

  def unhook_method
    class << self
      alias_method(:method_missing, @restore_stack.pop)
    end
  end

  public

  def parse(method = nil, actions = nil, &block)

    actions ||= block
    
    hook_method(method)

    # Execute
    if actions.is_a? Proc
      begin
        require 'parse_tree_extensions'
        require 'parse_tree'
        require 'ruby2ruby'
        @config[@@method_name] = actions.to_ruby.collect[1..-2].join
      rescue Exception
        @config[@@method_name] = NoRuby2Ruby.new "The gem ruby2ruby is not installed. It will not work on ruby 1.9."
      end
      
      instance_eval &actions
    elsif File.exist?(actions)
      
      @config[@@method_name] = File.open(actions).read

      instance_eval Open.read(actions), actions
    end

    unhook_method

  end


  # Processes a DSL. +method+ is the name of the method executed instead
  # of method_missing. The code to be evaluated as a DSL is either
  # specified in +&block+ or in the file pointed by +file+.
  def load_config(method = nil, file = nil, &block)
    @config = {}
    if file
      raise ConfigFileMissingError.new "Config file '#{ file }' is missing" unless File.exist? file
      parse(method, file)
    end

    if block
      parse(method, block)
    end
  end

  def config(action = nil)
    if action
      config = @config[action.to_sym]
    else
      config = @config[:DSL_action]
    end

    raise config if NoRuby2Ruby === config

    config
  end
end


