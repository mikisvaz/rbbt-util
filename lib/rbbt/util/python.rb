require 'rbbt-util'
require 'rbbt/util/python/paths'
require 'rbbt/util/python/run'
require 'rbbt/util/python/util'
require 'rbbt/util/python/script'

require 'pycall/import'

module RbbtPython
  extend PyCall::Import

  class RbbtPythonException < StandardError; end

  class Binding
    include PyCall::Import

    def run(*args, &block)
      instance_exec(*args, &block)
    end
  end

  def self.init_rbbt
    if ! defined?(@@__init_rbbt_python) || ! @@__init_rbbt_python
      add_paths(Rbbt.python.find_all)
      res = RbbtPython.run do
        Log.debug "Loading python 'rbbt' module into pycall RbbtPython module"
        pyimport("rbbt")
      end
      @@__init_rbbt_python = true
    end
  end

  def self.import_method(module_name, method_name, as = nil)
    RbbtPython.pyfrom module_name, import: method_name
    RbbtPython.method(method_name)
  end

  def self.call_method(module_name, method_name, *args)
    RbbtPython.import_method(module_name, method_name).call(*args)
  end
  
  def self.get_module(module_name)
    save_module_name = module_name.to_s.gsub(".", "_")
    RbbtPython.pyimport(module_name, as: save_module_name)
    RbbtPython.send(save_module_name)
  end

  def self.get_class(module_name, class_name)
    mod = get_module(module_name)
    mod.send(class_name)
  end

  def self.class_new_obj(module_name, class_name, args={})
    RbbtPython.get_class(module_name, class_name).new(**args)
  end

  def self.exec(script)
    PyCall.exec(script)
  end

  def self.iterate_index(elem, options = {})
    bar = options[:bar]

    len = PyCall.len(elem)
    case bar
    when TrueClass
      bar = Log::ProgressBar.new nil, :desc => "RbbtPython iterate"
    when String
      bar = Log::ProgressBar.new nil, :desc => bar
    end

    len.times do |i|
      begin
        yield elem[i]
        bar.tick if bar
      rescue PyCall::PyError
        if $!.type.to_s == "<class 'StopIteration'>"
          break
        else
          raise $!
        end
      rescue
        bar.error if bar
        raise $!
      end
    end

    Log::ProgressBar.remove_bar bar if bar
    nil
  end

  def self.iterate(iterator, options = {}, &block)
    if ! iterator.respond_to?(:__next__)
      if iterator.respond_to?(:__iter__)
        iterator = iterator.__iter__
      else
        return iterate_index(iterator, options, &block)
      end
    end

    bar = options[:bar]

    case bar
    when TrueClass
      bar = Log::ProgressBar.new nil, :desc => "RbbtPython iterate"
    when String
      bar = Log::ProgressBar.new nil, :desc => bar
    end

    while true
      begin
        elem = iterator.__next__
        yield elem
        bar.tick if bar
      rescue PyCall::PyError
        if $!.type.to_s == "<class 'StopIteration'>"
          break
        else
          raise $!
        end
      rescue
        bar.error if bar
        raise $!
      end
    end

    Log::ProgressBar.remove_bar bar if bar
    nil
  end

  def self.collect(iterator, options = {}, &block)
    acc = []
    self.iterate(iterator, options) do |elem|
      res = block.call elem
      acc << res
    end
    acc
  end

  def self.new_binding
    Binding.new
  end

  def self.binding_run(binding = nil, *args, &block)
    binding = new_binding
    binding.instance_exec *args, &block
  end
end
