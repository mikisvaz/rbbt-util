module Misc
  def self.lock(*args, &block)
    Open.lock(*args, &block)
  end

  def self.sensiblewrite(*args, &block)
    Open.sensible_write(*args, &block)
  end

  def self.common_path(*args, &block)
    Open.sensible_write(*args, &block)
  end
end
