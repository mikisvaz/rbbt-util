require 'rbbt/util/open'
require 'rbbt/util/rake'

module Resource
  class << self
    attr_accessor :resources, :rake_dirs
  end

  attr_accessor :pkgdir, :base

  def self.extended(base)
    base.base = base
  end

  def self.caller_lib_dir(file = nil)
    file = caller.reject{|l| l =~ /\/resource.rb/ }.first.sub(/\.rb.*/,'.rb') if file.nil?

    file = File.expand_path file
    while file != '/'
      dir = File.dirname file
      return dir if File.exists? File.join(dir, 'lib')
      file = File.dirname file
    end
    return nil
  end

  def self.resolve(path, pkgdir, type = :find)
    location, subpath = path.match(/(.*?)\/(.*)/).values_at 1, 2

    case type.to_sym
    when :user
      pkgdir = 'rbbt' if pkgdir.nil? or pkgdir.empty?
      File.join(ENV['HOME'], '.' + pkgdir, location, subpath)
    when :local
      File.join('/usr/local', location, pkgdir, subpath)
    when :global
      File.join('/', location, pkgdir, subpath)
    when :lib
      lib_dir = caller_lib_dir
      raise "Root of library not found" if lib_dir.nil?
      File.join(lib_dir, location, pkgdir, subpath)
    when :find
      %w(user local global lib).each do |_type|
        file = resolve(path, pkgdir, _type.to_sym)
        return file if File.exists? file
      end

      resolve(path, pkgdir, :user)
    end 
  end

  def self.define_rake(path, rakefile)
    @rake_dirs ||= {}
    @rake_dirs[path.find] = rakefile
  end

  def self.define_resource(path, type, content)
    @resources ||= {}
    @resources[path.find] = [type, content]
  end

  def self.produce(resource)
    resource = resource.find if Path === resource
    return resource if File.exists? resource

    case
    when @resources.include?(resource)
      type, content = @resources[resource]
      
      case type
      when :string
        Open.write(resource, content)
      when :url
        Open.write(resource, Open.read(content))
      when :install
        CMD.cmd("chmod +xr #{ content }")
        CMD.cmd([content, resource] * " ")
      end
      resource

    when @rake_dirs.select{|dir,rakefile| resource.in_dir?(dir)}.any?
      dir, rakefile = @rake_dirs.select{|dir,rakefile| resource.in_dir?(dir)}.first
      file = resource.sub(dir, '').sub(/^\//,'')
      FileUtils.mkdir_p dir unless File.exists? dir
      RakeHelper.run(rakefile, file, dir)
      resource
    end
  end

  module Path
    attr_accessor :pkgdir

    def self.path(name = nil, pkgdir = nil)
      name = name.nil? ? "" : name.to_s
      name.extend Path
      name.pkgdir = pkgdir
      name
    end

    def find(type = :find)
      return  self if pkgdir.nil?
      Path.path(Resource.resolve(self, pkgdir, type), nil)
    end

    def produce
      Resource.produce self.find
    end

    def join(name)
      Path.path(File.join(self, name.to_s), pkgdir)
    end

    def [](name)
      join name
    end
    
    def method_missing(name, *args)
      join name
    end

    def open
      Resource.produce self.find
      Open.open self.find
    end

    def read
      Resource.produce self.find
      Open.read self.find
    end

    def write(content)
      FileUtils.mkdir_p File.dirname(self.find) unless File.exists? self.find
      Open.write(self.find, content)
    end

    def define_as_string(content)
      Resource.define_resource(self, :string, content)
    end
    
    def define_as_url(url)
      Resource.define_resource(self, :url, url)
    end
 
    def define_as_rake(rakefile)
      Resource.define_rake(self, rakefile)
    end
 
    def define_as_install(install_file)
      Resource.define_resource(self, :install, install_file.find)
    end

    def in_dir?(dir)
      ! ! File.expand_path(self).match(/^#{Regexp.quote dir}/)
    end

    def to_s
      self.find
    end
  end

  def method_missing(name)
    pkgdir = base == Rbbt ? '' : base.to_s.downcase
    Path.path(name, pkgdir)
  end
end
