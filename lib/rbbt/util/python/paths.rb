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

  def self.process_paths
    RbbtPython.run_direct 'sys' do
      RbbtPython.paths.each do |path|
        sys.path.append path
      end
      nil
    end
  end
end
