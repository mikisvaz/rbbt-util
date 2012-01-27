require 'rbbt/util/chain_methods'
require 'rbbt/resource/util'
require 'rbbt/tsv'

module Path
  attr_accessor :resource, :pkgdir

  extend ChainMethods
  self.chain_prefix = :path

  def self.setup(string, pkgdir = nil, resource = nil)
    string.extend Path
    string.pkgdir = pkgdir || 'rbbt'
    string.resource = resource
    string
  end

  def self.extended(string)
    setup_chains(string)
    if not string.respond_to? :byte
      class << string
        alias byte path_clean_get_brackets
      end
    end
  end

  def join(name)
    if self.empty?
      Path.setup name.to_s, @pkgdir, @resource
    else
      Path.setup File.join(self, name.to_s), @pkgdir, @resource
    end
  end

  def dirname
    Path.setup File.dirname(self), @pkgdir, @resource
  end

  def path_get_brackets(name)
    join name
  end

  def path_method_missing(name, prev = nil, *args, &block)
    if block_given?
      path_clean_method_missing name, prev, *args, &block
    else
      # Fix problem with ruby 1.9 calling methods by its own initiative. ARG
      path_clean_method_missing(name, prev, *args) if name.to_s =~ /^to_/
        if prev.nil?
          join name
        else
          join(prev).join(name)
        end
    end
  end

  SEARCH_PATHS = {
    :user => File.join(ENV['HOME'], ".{PKGDIR}", "{TOPLEVEL}", "{SUBPATH}"),
    :global => File.join('/', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :local => File.join('/usr/local', "{TOPLEVEL}", "{PKGDIR}", "{SUBPATH}"),
    :lib => File.join('{LIBDIR}', "{TOPLEVEL}", "{SUBPATH}"),
    :default => :user
  }

  def find(where = nil, caller_lib = nil, search_paths = nil)
    where = search_paths[:default] if where == :default
    search_paths ||= SEARCH_PATHS
    return self if located?
    if self.match(/(.*?)\/(.*)/)
      toplevel, subpath = self.match(/(.*?)\/(.*)/).values_at 1, 2
    else
      toplevel, subpath = self, ""
    end

    path = nil
    if where.nil?
      search_paths.keys.each do |w| 
        path = find(w, caller_lib, search_paths)
        return path if File.exists? path
      end
      if search_paths.include? :default
        find((search_paths[:default] || :user), caller_lib, search_paths)
      else
        raise "Path '#{ path }' not found, and no default specified in search paths: #{search_paths.inspect}"
      end
    else
      libdir = where == :lib ? Path.caller_lib_dir(caller_lib) : ""
      libdir ||= ""
      Path.setup search_paths[where].sub('{PKGDIR}', pkgdir).sub('{TOPLEVEL}', toplevel).sub('{SUBPATH}', subpath).sub('{LIBDIR}', libdir), @pkgdir, @resource
    end
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

  def produce
    path = self.find
    return self if File.exists? path

    raise "No resource defined to produce file: #{ self }" if resource.nil?

    resource.produce path

    path
  end

  def read
    Open.read(self.produce.find)
  end

  def open
    Open.open(self.produce.find)
  end

  def to_s
    "" + self
  end

  def tsv(*args)
    TSV.open(self.produce, *args)
  end

  def list
    Open.read(self.produce.find).split "\n"
  end

  def keys(field = 0, sep = "\t")
    Open.read(self.produce.find).split("\n").collect{|l| next if l =~ /^#/; l.split(sep, -1)[field]}.compact
  end

  def yaml
    YAML.load self.open
  end

  def index(options = {})
    TSV.index(self.produce.find, options)
  end

  def range_index(start, eend, options = {})
    TSV.range_index(self.produce.find, start, eend, options)
  end

  def pos_index(pos, options = {})
    TSV.pos_index(self.produce.find, pos, options)
  end

  def to_yaml(*args)
    self.to_s.to_yaml(*args)
  end

  def fields
    TSV.parse_header(self.open).fields
  end

  def all_fields
    TSV.parse_header(self.open).all_fields
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
end
