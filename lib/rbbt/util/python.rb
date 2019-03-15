require 'rbbt-util'

require 'pycall/import'

module RbbtPython
  extend PyCall::Import
  def self.run(mod = nil, &block)
    begin
      pyimport mod unless mod.nil?
      case block.arity
      when 0
        yield 
      when 1
        yield self.send(mod)
      else
        raise "Unknown arity on block of code #{block.arity}"
      end
    rescue
      Log.exception $!
    end
  end
end
