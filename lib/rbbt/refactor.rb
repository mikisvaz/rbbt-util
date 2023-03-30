require 'scout/log'

module Rbbt
  def self.require_instead(new_file)
    Log.low "Requiring #{new_file} instead of #{caller.first}"
    require new_file
  end
end
