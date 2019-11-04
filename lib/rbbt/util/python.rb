require 'rbbt-util'

require 'pycall/import'

module RbbtPython
  extend PyCall::Import
  def self.run(mod = nil, imports = nil, &block)
    begin
      if mod
        if imports
          pyfrom mod, :import => imports
        else
          pyimport mod 
        end
      end

      module_eval(&block)

    rescue
      Log.exception $!
    end
  end

  def self.add_path(path)
    self.run 'sys' do
      sys.path.append path
    end
  end

end
