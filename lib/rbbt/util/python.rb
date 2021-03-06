require 'rbbt-util'
require 'pycall/import'

module RbbtPython
  extend PyCall::Import

  def self.exec(script)
    PyCall.exec(script)
  end

  def self.iterate(iterator, options = {})
    bar = options[:bar]

    case bar
    when TrueClass
      bar = Log::ProgressBar.new nil, :desc => "RbbtPython iterate"
    when String
      bar = Log::ProgressBar.new nil, :desc => bar
    end

    while true
      begin
        yield iterator.__next__
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
      if Array === imports
        pyfrom mod, :import => imports
      elsif Hash === imports
        pyimport mod, imports
      else
        pyimport mod 
      end
    end

    module_eval(&block)
  end

  def self.run_log(mod = nil, imports = nil, severity = 0, severity_err = nil, &block)
    if mod
      if Array === imports
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

  def self.add_path(path)
    self.run 'sys' do
      sys.path.append path
    end
  end

  def self.add_paths(paths)
    self.run 'sys' do
      paths.each do |path|
        sys.path.append path
      end
    end
  end

  RbbtPython.add_paths Rbbt.python.find_all
  RbbtPython.pyimport "rbbt"
end
