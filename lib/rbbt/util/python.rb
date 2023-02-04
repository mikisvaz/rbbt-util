require 'rbbt-util'
require 'pycall/import'
require 'rbbt/util/python/util'

module RbbtPython
  extend PyCall::Import

  class RbbtPythonException < StandardError; end

  class Binding
    include PyCall::Import

    def run(*args, &block)
      instance_exec(*args, &block)
    end
  end

  def self.script(text, options = {})
    Log.debug "Running python script:\n#{text.dup}"
    text = StringIO.new text unless IO === text
    CMD.cmd_log(:python, options.merge(:in => text))
  end

  def self.add_path(path)
    begin
      self.run 'sys' do
        sys.path.append path
      end
    rescue
      raise RbbtPythonException, 
        "Could not add path #{Misc.fingerprint path} to python sys: " + $!.message
    end
  end

  def self.add_paths(paths)
    self.run 'sys' do
      paths.each do |path|
        sys.path.append path
      end
    end
  end

  def self.init_rbbt
    if ! defined?(@@__init_rbbt_python) || ! @@__init_rbbt_python
      Log.debug "Loading python 'rbbt' module into pycall RbbtPython module"
      RbbtPython.add_paths(Rbbt.python.find_all)
      RbbtPython.pyimport("rbbt")
      @@__init_rbbt_python = true
    end
  end

  def self.import_method(module_name, method_name, as = nil)
    RbbtPython.pyfrom module_name, import: method_name
    RbbtPython.method(method_name)
  end

  def self.exec(script)
    PyCall.exec(script)
  end

  def self.iterate_index(elem, options = {})
    iii :interate_index
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

  def self.run(mod = nil, imports = nil, &block)
    if mod
      if Hash === imports
        pyimport mod, **imports
      elsif imports.nil?
        pyimport mod 
      else
        pyfrom mod, :import => imports
      end
    end

    module_eval(&block)
  end

  def self.run_log(mod = nil, imports = nil, severity = 0, severity_err = nil, &block)
    if mod
      if imports == "*" || imports == ["*"]
        pyfrom mod
      elsif Array === imports
        pyfrom mod, :import => imports
      elsif Hash === imports
        pyimport mod, imports
      else
        pyimport mod 
      end
    end

    Log.trap_std("Python STDOUT", "Python STDERR", severity, severity_err) do
      module_eval(&block)
    end
  end

  def self.run_log_stderr(mod = nil, imports = nil, severity = 0, &block)
    if mod
      if Array === imports
        pyfrom mod, :import => imports
      elsif Hash === imports
        pyimport mod, imports
      else
        pyimport mod 
      end
    end

    Log.trap_stderr("Python STDERR", severity) do
      module_eval(&block)
    end
  end

  def self.new_binding
    Binding.new
  end

  def self.binding_run(binding = nil, *args, &block)
    binding = new_binding
    binding.instance_exec *args, &block
  end
end
