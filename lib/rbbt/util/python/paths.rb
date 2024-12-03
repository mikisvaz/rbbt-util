module RbbtPython
  class << self
    attr_accessor :paths
    def paths
      @paths ||= []
    end
  end
  def self.add_path(path)
    self.paths << path
  end

  def self.add_paths(paths)
    self.paths.concat paths
  end
end
