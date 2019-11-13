require 'rbbt-util'
require 'pycall/import'

module RbbtPython
  extend PyCall::Import
  def self.run(mod = nil, imports = nil, &block)
    if mod
      if imports
        pyfrom mod, :import => imports
      else
        pyimport mod 
      end
    end

    module_eval(&block)
  end

  def self.run_log(mod = nil, imports = nil, severity = 0, severity_err = nil, &block)
    if mod
      if imports
        pyfrom mod, :import => imports
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
      if imports
        pyfrom mod, :import => imports
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

end
