require 'rbbt/resource/util'
require 'rbbt/util/misc/indiferent_hash'
require 'yaml'

module Path
  attr_accessor :resource, :pkgdir, :original, :search_paths, :search_order, :libdir, :where

  def self.setup(string, pkgdir = nil, resource = nil, search_paths = nil, search_order = nil, libdir = nil)
    return string if string.nil?
    string = string.dup if string.frozen?
    string.extend Path
    string.pkgdir = pkgdir || 'rbbt'
    string.resource = resource
    string.search_paths = search_paths
    string.search_order = search_order
    string.libdir = libdir || Path.caller_lib_dir 
    string
  end

  def search_order
    @search_order ||= STANDARD_SEARCH.dup.uniq
  end

  def search_paths
    @search_paths ||= SEARCH_PATHS.dup
  end

  def add_search_path(name, dir)
    search_paths[name.to_sym] = dir
  end

  def prepend_search_path(name, dir)
    add_search_path(name, dir)
    search_order.unshift(name.to_sym)
  end

  def append_search_path(name, dir)
    add_search_path(name, dir)
    search_order.push(name.to_sym)
  end

  def sub(*args)
    self.annotate super(*args)
  end

  def annotate(name)
    name = name.to_s
    name = Path.setup name, @pkgdir, @resource, @search_paths, @search_order, @libdir
    name
  end

  def join(name)
    raise "Invalid path: #{ self }" if self.nil?
    new = if self.empty?
            self.annotate name.to_s.dup.chomp
          else
            self.annotate File.join(self, name.to_s.chomp)
          end
    new.original = File.join(self.original, name.to_s.chomp) if self.original
    new
  end

  def dirname
    Path.setup File.dirname(self), @pkgdir, @resource
  end

  def directory?
    return nil unless self.exists?
    File.directory? self.find 
  end

  def glob(pattern = '*')
    if self.include? "*"
      self.glob_all pattern
    else
      return [] unless self.exists? 
      found = self.find
      exp = File.join(found, pattern)
      paths = Dir.glob(exp).collect{|f| self.annotate(f) }

      paths.each do |p|
        p.original = File.join(found.original, p.sub(/^#{found}/, ''))
      end if found.original

      paths
    end
  end

  def glob_all(pattern = nil, caller_lib = nil, search_paths = nil)
    search_paths ||= @search_paths || SEARCH_PATHS
    search_paths = search_paths.dup

    location_paths = {}
    search_paths.keys.collect do |where| 
      found = find(where, Path.caller_lib_dir, search_paths)
      paths = pattern ? Dir.glob(File.join(found, pattern)) : Dir.glob(found) 

      paths = paths.collect{|p| self.annotate p }

      paths = paths.each do |p|
        p.original = File.join(found.original, p.sub(/^#{found}/, ''))
        p.where = where
      end if found.original and pattern

      location_paths[where] = paths
    end

    #location_paths.values.compact.flatten.collect{|file| File.expand_path(file) }.uniq.collect{|path| Path.setup(path, self.resource, self.pkgdir)}
    location_paths.values.compact.flatten.uniq
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

  SEARCH_PATHS = IndiferentHash.setup({
    :current => File.join("{PWD}", "{TOPLEVEL}", "{SUBPATH}"),
    :user    => File.join(ENV['HOME'], ".{PKGDIR}", "{TOPLEVEL}", "{SUBPATH}"),
    :global  => File.join('/', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :local   => File.join('/usr/local', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :fast   => File.join('/fast', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :cache   => File.join('/cache', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :bulk   => File.join('/bulk', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :lib     => File.join('{LIBDIR}', "{TOPLEVEL}", "{SUBPATH}"),
    :base   => File.join(caller_lib_dir(__FILE__), "{TOPLEVEL}", "{SUBPATH}"),
    :default => :user
  })

  STANDARD_SEARCH = %w(current workflow user local global lib fast cache bulk)

  search_path_file = File.join(ENV['HOME'], '.rbbt/etc/search_paths')
  if File.exist?(search_path_file)
    begin
      Misc.load_yaml(search_path_file).each do |where, location|
        SEARCH_PATHS[where.to_sym] = location
      end
    rescue
      Log.error "Error loading search_paths from #{search_path_file}: " << $!.message
    end
  end

  def find(where = nil, caller_lib = nil, paths = nil)

    if located?
      self.original ||= self
      return self
    end

    if where == :all || where == 'all'
      return find_all(caller_lib, paths)
    end

    @path ||= {}
    rsearch_paths = (resource and resource.respond_to?(:search_paths)) ? resource.search_paths : nil 
    key = [where, caller_lib, rsearch_paths, paths].inspect
    self.sub!('~/', Etc.getpwuid.dir + '/') if self.include? "~"

    return @path[key] if @path[key]

    @path[key] ||= begin
                     paths = [paths, rsearch_paths, self.search_paths, SEARCH_PATHS].reverse.compact.inject({}){|acc,h| acc.merge! h; acc }
                     where = paths[:default] if where == :default
                     if self.match(/(.*?)\/(.*)/)
                       toplevel, subpath = self.match(/(.*?)\/(.*)/).values_at 1, 2
                     else
                       toplevel, subpath = "{REMOVE}", self
                     end

                     path = nil
                     search_order = self.search_order || []
                     res = nil
                     if where.nil?

                       (STANDARD_SEARCH - search_order).each do |w| 
                         w = w.to_sym
                         break if res
                         next unless paths.include? w
                         path = find(w, caller_lib, paths)
                         res = path if File.exist? path
                       end

                       search_order.each do |w| 
                         w = w.to_sym
                         next if res
                         next unless paths.include? w
                         path = find(w, caller_lib, paths)
                         res = path if File.exist?(path)
                       end if res.nil?

                       (paths.keys - STANDARD_SEARCH - search_order).each do |w|
                         w = w.to_sym
                         next if res
                         next unless paths.include? w
                         path = find(w, caller_lib, paths)
                         res = path if File.exist? path
                       end if res.nil?

                       if paths.include? :default
                         res = find((paths[:default] || :user), caller_lib, paths)
                       else
                         raise "Path '#{ path }' not found, and no default specified in search paths: #{paths.inspect}"
                       end if res.nil?

                     else
                       where = where.to_sym

                       if paths.include? where
                         path = paths[where]
                       elsif where.to_s.include?("/")
                         path = where.to_s
                       else
                         raise "Did not recognize the 'where' tag: #{where}. Options: #{paths.keys}" unless paths.include? where
                       end

                       if where == :lib 
                         libdir = @libdir || Path.caller_lib_dir(caller_lib) || "NOLIBDIR" 
                       else
                         libdir = "NOLIBDIR"
                       end

                       pwd = FileUtils.pwd
                       path = File.join(path, "{PATH}") unless path.include? "PATH}" or path.include? "{BASENAME}"
                       path = path.
                         sub('{PKGDIR}', pkgdir).
                         sub('{PWD}', pwd).
                         sub('{TOPLEVEL}', toplevel).
                         sub('{SUBPATH}', subpath).
                         sub('{BASENAME}', File.basename(self)).
                         sub('{PATH}', self).
                         sub('{LIBDIR}', libdir).
                         sub('{RESOURCE}', resource.to_s).
                         sub('{REMOVE}/', '').
                         sub('{REMOVE}', '')

                       path = path + '.gz' if File.exist?(path + '.gz')
                       path = path + '.bgz' if File.exist?(path + '.bgz')

                       self.annotate path

                       res = path
                     end

                     res.original = self.original || self
                     res.where = where

                     res
                   end
    @path[key]
  end

  def find_all(caller_lib = nil, search_paths = nil)
    search_paths ||= @search_paths || SEARCH_PATHS
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

  def _exists?
    Open.exists? self.find.to_s
  end

  def exists?
    begin
      self.produce
      _exists?
    rescue Exception
      false
    end
  end

  def produce(force = false)
    return self if _exists? and not force

    raise "No resource defined to produce file: #{ self }" if resource.nil?

    resource.produce self, force if Resource === resource

    self
  end

  def read(&block)
    Open.read(self.produce.find, &block)
  end

  def write(*args, &block)
    Open.write(self.produce.find, *args, &block)
  end


  def open(options = {}, &block)
    file = Open.remote?(self) || Open.ssh?(self) ? self : self.produce.find
    Open.open(file, options, &block)
  end

  def to_s
    "" + self
  end

  def basename
    Path.setup(File.basename(self), self.resource, self.pkgdir)
  end

  def tsv(*args)
    begin
      path = self.produce
    rescue Resource::ResourceNotFound => e
      begin
        path = self.set_extension('tsv').produce
      rescue Resource::ResourceNotFound 
        raise e
      end
    end
    TSV.open(path, *args)
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
      YAML.respond_to?(:unsafe_load) ? YAML.unsafe_load(f) : YAML.load(f)
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
    new_path = self + "." + new_extension.to_s
    self.annotate(new_path)
  end

  def remove_extension(new_extension = nil)
    self.sub(/\.[^\.\/]{1,5}$/,'')
  end


  def self.get_extension(path)
    path.match(/\.([^\.\/]{1,5})$/)[1]
  end

  def replace_extension(new_extension = nil, multiple = false)
    if String === multiple
      new_path = self.sub(/(\.[^\.\/]{1,5})(.#{multiple})?$/,'')
    elsif multiple
      new_path = self.sub(/(\.[^\.\/]{1,5})+$/,'')
    else
      new_path = self.sub(/\.[^\.\/]{1,5}$/,'')
    end
    new_path = new_path + "." + new_extension.to_s
    self.annotate(new_path)
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

      if not File.exist?(file)
        file= Dir.glob(file.sub(/\.[^\.\/]+$/, '.*')).first
      end

      Path.setup file, @pkgdir, @resource
    else
      relative_file = self.sub(/^doc\//, '\1')

      if not File.exist?(relative_file)
        relative_file = Dir.glob(relative_file.sub(/\.[^\.\/]+$/, '.*')).first
      end

      Path.setup relative_file , @pkgdir, @resource
    end
  end

  def clean_annotations
    "" << self.to_s
  end

end
