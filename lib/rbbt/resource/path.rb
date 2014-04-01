require 'rbbt/resource/util'
require 'yaml'

module Path
  attr_accessor :resource, :pkgdir, :search_paths

  def self.setup(string, pkgdir = nil, resource = nil, search_paths = nil)
    string.extend Path
    string.pkgdir = pkgdir || 'rbbt'
    string.resource = resource
    string.search_paths = search_paths
    string
  end

  def sub(*args)
    self.annotate super(*args)
  end

  def annotate(name)
    Path.setup name.to_s, @pkgdir, @resource, @search_paths
  end

  def join(name)
    raise "Invalid path: #{ self }" if self.nil?
    if self.empty?
      self.annotate name.to_s.dup
    else
      self.annotate File.join(self, name.to_s)
    end
  end

  def dirname
    Path.setup File.dirname(self), @pkgdir, @resource
  end

  def directory?
    return nil unless self.exists?
    File.directory? self.find 
  end

  def glob(pattern = '*')
    return [] unless self.exists?
    exp = File.join(self.find, pattern)
    Dir.glob(exp).collect{|f| Path.setup(f, self.resource, self.pkgdir)}
  end

  def [](name, orig = false)
    return super(name) if orig
    join name
  end

  def byte(pos)
    send(:[], pos, true)
  end

  def method_missing(name, prev = nil, *args, &block)
    if block_given?
      super name, prev, *args, &block
    else
      # Fix problem with ruby 1.9 calling methods by its own initiative. ARG
      super(name, prev, *args) if name.to_s =~ /^to_/
      if prev.nil?
        join name
      else
        join(prev).join(name)
      end
    end
  end

  SEARCH_PATHS = {
    :current => File.join("{PWD}", "{TOPLEVEL}", "{SUBPATH}"),
    :user    => File.join(ENV['HOME'], ".{PKGDIR}", "{TOPLEVEL}", "{SUBPATH}"),
    :global  => File.join('/', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :local   => File.join('/usr/local', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :lib     => File.join('{LIBDIR}', "{TOPLEVEL}", "{SUBPATH}"),
    :default => :user
  }

  search_path_file = File.join(ENV['HOME'], '.rbbt/etc/search_paths')
  if File.exists?(search_path_file)
    YAML.load(File.open(search_path_file)).each do |where, location|
      SEARCH_PATHS[where.to_sym] = location
    end
  end

  def find(where = nil, caller_lib = nil, paths = nil)
    paths = (self.search_paths || SEARCH_PATHS).merge(paths || {})
    where = paths[:default] if where == :default
    return self if located?
    if self.match(/(.*?)\/(.*)/)
      toplevel, subpath = self.match(/(.*?)\/(.*)/).values_at 1, 2
    else
      toplevel, subpath = self, ""
    end

    path = nil
    if where.nil?
      %w(current user local global lib).each do |w| 
        w = w.to_sym
        next unless paths.include? w
        path = find(w, caller_lib, paths)
        return path if File.exists? path
      end
      if paths.include? :default
        find((paths[:default] || :user), caller_lib, paths)
      else
        raise "Path '#{ path }' not found, and no default specified in search paths: #{paths.inspect}"
      end
    else
      where = where.to_sym
      raise "Did not recognize the 'where' tag: #{where}. Options: #{paths.keys}" unless paths.include? where

      if where == :lib
        libdir = Path.caller_lib_dir(caller_lib) || "NOLIBDIR"
      else
        libdir = "NOLIBDIR"
      end
      pwd = FileUtils.pwd
      path = paths[where].
        sub('{PKGDIR}', pkgdir).
        sub('{PWD}', pwd).
        sub('{TOPLEVEL}', toplevel).
        sub('{SUBPATH}', subpath).
        sub('{PATH}', self).
        sub('{LIBDIR}', libdir) #, @pkgdir, @resource, @search_paths
      self.annotate path
    end
  end

  def find_all(caller_lib = nil, search_paths = nil)
    search_paths ||= self.search_paths || SEARCH_PATHS
    search_paths = search_paths.dup

    search_paths.keys.
      collect{|where| find(where, Path.caller_lib_dir, search_paths) }.
      compact.select{|file| file.exists? }.uniq
  end

  #{{{ Methods

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

  def produce(force = false)
    path = self.find

    return self if Open.exists?(path.to_s) and not force

    raise "No resource defined to produce file: #{ self }" if resource.nil?

    resource.produce self, force

    self
  end

  def read(&block)
    Open.read(self.produce.find, &block)
  end

  def write(*args, &block)
    Open.write(self.produce.find, *args, &block)
  end


  def open(options = {}, &block)
    Open.open(self.produce.find, options, &block)
  end

  def to_s
    "" + self
  end

  def basename
    Path.setup(File.basename(self), self.resource, self.pkgdir)
  end

  def tsv(*args)
    TSV.open(self.produce, *args)
  end

  def tsv_options(options = {})
    self.open do |stream|
      TSV::Parser.new(stream, options).options
    end
  end

  def traverse(options = {}, &block)
    TSV::Parser.traverse(self.open, options, &block)
  end

  def list
    Open.read(self.produce.find).split "\n"
  end

  def keys(field = 0, sep = "\t")
    Open.read(self.produce.find).split("\n").collect{|l| next if l =~ /^#/; l.split(sep, -1)[field]}.compact
  end

  def yaml
    self.open do |f|
      YAML.load f
    end
  end

  def pipe_to(cmd, options = {})
    CMD.cmd(cmd, {:in => self.open, :pipe => true}.merge(options))
  end

  def index(options = {})
    TSV.index(self.produce, options)
  end

  def range_index(start, eend, options = {})
    TSV.range_index(self.produce, start, eend, options)
  end

  def pos_index(pos, options = {})
    TSV.pos_index(self.produce, pos, options)
  end

  def to_yaml(*args)
    self.to_s.to_yaml(*args)
  end

  def fields
    TSV.parse_header(self.open).fields
  end

  def all_fields
    self.open do |stream|
      TSV.parse_header(stream).all_fields
    end
  end

  def identifier_file_path
    if self.dirname.identifiers.exists?
      self.dirname.identifiers
    else
      nil
    end
  end

  def identifier_files
    if identifier_file_path.nil?
      []
    else
      [identifier_file_path]
    end
  end

  def set_extension(new_extension = nil)
    new_path = self.sub(/\.[^\.\/]+$/, "." << new_extension.to_s)
    Path.setup new_path, @pkgdir, @resource
  end

  def doc_file(relative_to = 'lib')
    if located?
      lib_dir = Path.caller_lib_dir(self, relative_to)
      relative_file = File.join( 'doc', self.sub(lib_dir,''))
      Path.setup File.join(lib_dir, relative_file) , @pkgdir, @resource
    else
      Path.setup File.join('doc', self) , @pkgdir, @resource
    end
  end

  def source_for_doc_file(relative_to = 'lib')
    if located?
      lib_dir = Path.caller_lib_dir(Path.caller_lib_dir(self, 'doc'), relative_to)
      relative_file = self.sub(/(.*\/)doc\//, '\1').sub(lib_dir + "/",'')
      file = File.join(lib_dir, relative_file)

      if not File.exists?(file)
        file= Dir.glob(file.sub(/\.[^\.\/]+$/, '.*')).first
      end

      Path.setup file, @pkgdir, @resource
    else
      relative_file = self.sub(/^doc\//, '\1')

      if not File.exists?(relative_file)
        relative_file = Dir.glob(relative_file.sub(/\.[^\.\/]+$/, '.*')).first
      end

      Path.setup relative_file , @pkgdir, @resource
    end
  end
end
