require 'scout/log'
require 'scout/exceptions'

module Rbbt
  def self.require_instead(new_file)
    Log.low "Requiring #{new_file} instead of #{caller.first}"
    require new_file
  end

  def self.relay_module_method(new_mod, new_method, orig_mod, orig_method = nil)
    orig_method = new_method if orig_method.nil?
    method = orig_mod.method(orig_method)
    class << new_mod
      self
    end.define_method(new_method, &method)
  end
end

require 'rbbt/util/concurrency/processes/refactor'
require 'rbbt/util/named_array/refactor'
