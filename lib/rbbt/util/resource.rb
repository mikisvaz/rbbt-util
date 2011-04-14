require 'rbbt/util/open'
require 'rbbt/util/rake'

module Resource
  class << self
    attr_accessor :resources, :rake_dirs
  end

  def self.extended(base)
    class << base
      attr_accessor :pkgdir, :lib_dir, :base, :offsetdir, :namespace
    end
    base.base = base
    base.lib_dir = caller_lib_dir
    base.pkgdir = base.to_s.downcase unless base.to_s == "Rbbt"
  end

  def self.caller_base_dir(file = nil)
    file = caller.reject{|l| l =~ /\/util\/(?:resource\.rb|progress-monitor\.rb|workflow\.rb)/ }.first.sub(/\.rb.*/,'.rb') if file.nil?
    File.dirname(File.expand_path(file))
  end

  def self.caller_lib_dir(file = nil)
    file = caller.reject{|l| l =~ /\/util\/(?:resource\.rb|progress-monitor\.rb|workflow\.rb)/ }.first.sub(/\.rb.*/,'.rb') if file.nil?

    file = File.expand_path file
    while file != '/'
      dir = File.dirname file
      return dir if File.exists? File.join(dir, 'lib')
      file = File.dirname file
    end
    return nil
  end

  def self.resolve(path, pkgdir, type = :find, lib_dir = nil)
    if path.match(/(.*?)\/(.*)/)
      location, subpath = path.match(/(.*?)\/(.*)/).values_at 1, 2
    else
      location, subpath = path, ""
    end

    case type.to_sym
    when :user
      pkgdir = 'rbbt' if pkgdir.nil? or pkgdir.empty?
      File.join(ENV['HOME'], '.' + pkgdir, location, subpath)
    when :local
      File.join('/usr/local', location, pkgdir, subpath)
    when :global
      File.join('/', location, pkgdir, subpath)
    when :lib
      if not caller_lib_dir.nil? and not caller_lib_dir == "/"
        path = File.join(caller_lib_dir, location, subpath)
        return path if File.exists?(path) or lib_dir.nil?
      end
      raise "Root of library not found" if lib_dir.nil?
      File.join(lib_dir, location, subpath)
    when :find
      %w(user local global lib).each do |_type|
        file = resolve(path, pkgdir, _type.to_sym, lib_dir)
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

  def self.set_software_env(software_dir)
    bin_dir = File.join(software_dir, 'bin')
    opt_dir = File.join(software_dir, 'opt')

    Misc.env_add 'PATH', bin_dir

    FileUtils.mkdir_p opt_dir unless File.exists? opt_dir
    %w(.ld-paths .pkgconfig-paths .aclocal-paths .java-classpaths).each do |file|
      filename = File.join(opt_dir, file)
      FileUtils.touch filename unless File.exists? filename
    end

    if not File.exists? File.join(opt_dir,'.post_install')
      Open.write(File.join(opt_dir,'.post_install'),"#!/bin/bash\n")
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
      Misc.env_add('LD_RUN_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.pkgconfig-paths').split(/\n/).each do |line|
      Misc.env_add('PKG_CONFIG_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.ld-paths').split(/\n/).each do |line|
      Misc.env_add('LD_LIBRARY_PATH',line.chomp)
    end

    Open.read(File.join opt_dir, '.aclocal-paths').split(/\n/).each do |line|
      Misc.env_add('ACLOCAL_FLAGS', "-I#{File.join(opt_dir, line.chomp)}", ' ')
    end

    Open.read(File.join opt_dir, '.java-classpaths').split(/\n/).each do |line|
      Misc.env_add('CLASSPATH', "#{File.join(opt_dir,'java', 'lib', line.chomp)}")
    end

    Dir.glob(File.join opt_dir, 'jars', '*').each do |file|
      Misc.env_add('CLASSPATH', "#{File.expand_path(file)}")
    end

    File.chmod 0774, File.join(opt_dir, '.post_install')

    CMD.cmd(File.join(opt_dir, '.post_install'))
  end

  def self.produce(resource)
    resource = resource.find if Path === resource
    return resource if File.exists? resource

    @resources ||= {}
    @rake_dirs ||= {}
    case
    when @resources.include?(resource)
      type, content = @resources[resource]
      
      case type
      when :string
        Open.write(resource, content)
      when :url
        Open.write(resource, Open.read(content))
      when :proc
        Open.write(resource, content.call)
      when :install
        software_dir = File.dirname(File.dirname(resource.to_s))
        preamble = <<-EOF
#!/bin/bash

RBBT_SOFTWARE_DIR="#{software_dir}"

INSTALL_HELPER_FILE="#{Rbbt.share.install.software.lib.install_helpers.find :lib, caller_lib_dir(__FILE__)}"
source "$INSTALL_HELPER_FILE"
        EOF

        CMD.cmd('bash', :in => preamble + "\n" + content.read)
        set_software_env(software_dir)
      else
        raise "Could not produce #{ resource }. (#{ type }, #{ content })"
      end
      resource

    when @rake_dirs.select{|dir,rakefile| resource.in_dir?(dir)}.any?
      dir, rakefile = @rake_dirs.select{|dir,rakefile| resource.in_dir?(dir)}.first
      file = resource.sub(dir, '').sub(/^\//,'')
      rakefile = rakefile.find if Resource::Path === rakefile
      dir = dir.find if Resource::Path === dir
      FileUtils.mkdir_p dir unless File.exists? dir
      RakeHelper.run(rakefile, file, dir)
      resource
    end
  end

  def relative_to(klass, path)
    self.offsetdir = path
    if Rbbt == klass
      self.pkgdir = ""
    else
      self.pkgdir = klass.to_s.downcase
    end
  end

  def data_module(klass)
    relative_to klass, "share/#{self.to_s.downcase}" unless klass == base
    rakefile = klass.share.install[self.to_s].Rakefile
    rakefile.lib_dir = Resource.caller_lib_dir


    self[''].define_as_rake rakefile
    self.namespace = base.to_s
    self.lib_dir = Resource.caller_lib_dir
  end

  module Path
    attr_accessor :pkgdir, :namespace, :lib_dir

    def self.path(name = nil, pkgdir = nil, namespace = nil, lib_dir = nil)
      name = name.nil? ? "" : name.to_s
      name.extend Path
      name.pkgdir = pkgdir
      name.namespace = namespace
      name.lib_dir = lib_dir
      name
    end

    def find(type = :find, lib_dir = nil)
      lib_dir ||= @lib_dir
      return  self if pkgdir.nil?
      path = Path.path(Resource.resolve(self, pkgdir, type, lib_dir), nil)
      path.namespace = namespace
      path
    end

    def produce
      Resource.produce self.find
    end

    def dirname
      Path.path(File.dirname(self), pkgdir, namespace, lib_dir)
    end

    def join(name)
      Path.path(File.join(self, name.to_s), pkgdir, namespace, lib_dir)
    end

    def [](name)
      join name
    end
    
    alias :old_method_missing :method_missing
    def method_missing(name, prev = nil, *args)
      old_method_missing(name, prev, *args) if name.to_s =~ /^to_/
      join prev unless prev.nil?
      join name
    end

    def open(*args)
      Resource.produce self.find
      Open.open self.find, *args
    end

    def read(*args)
      Resource.produce self.find
      Open.read self.find, *args
    end

    def yaml(*args)
      YAML.load(open)
    end

    def marshal(*args)
      Marshal.load(open)
    end

    def write(content, *args)
      FileUtils.mkdir_p File.dirname(self.find) unless File.exists? self.find
      Open.write(self.find, content, *args)
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
    
    def define_as_proc(&block)
      Resource.define_resource(self, :proc, &block)
    end
 
 
    def define_as_install(install_file)
      Resource.define_resource(self, :install, install_file.find)
      self.produce
      software_dir = File.dirname(File.dirname(self.to_s))
      Resource.set_software_env(software_dir)
    end

    def in_dir?(dir)
      ! ! File.expand_path(self).match(/^#{Regexp.quote dir}/)
    end

    def to_s
      self.find
    end

    def filename
      self.find
    end

    def exists?
      begin
        self.produce
        File.exists? self.find
      rescue
        false
      end
    end
  end

  module WithKey
    def self.extended(base)
      class << base
        attr_accessor :klass, :key
      end
    end

    alias :old_method_missing :method_missing
    def method_missing(name, *args)
      return old_method_missing(name, *args) if name.to_s =~ /^to_/
      if key
        klass.send(name, key, *args)
      else
        klass.send(name, *args)
      end
    end
  end

  def with_key(key)
    klass = self
    o     = Object.new
    o.extend WithKey
    o.klass = self
    o.key   = key
    o
  end

  def [](name)
    if pkgdir.nil?
      @pkgdir = (base == Rbbt ? '' : base.to_s.downcase) 
    end
    name = File.join(offsetdir.to_s, name.to_s) unless offsetdir.nil? or offsetdir.empty?
    Path.path(name, pkgdir, namespace, lib_dir)
  end

  alias :old_method_missing :method_missing
  def method_missing(name, prev = nil, *args)
    return old_method_missing(name, prev, *args) if name.to_s =~ /^to_/
    if prev
      self[prev][name]
    else
      self[name]
    end
  end
end

def resource_path(path)
  Resource::Path.path(path)
end
