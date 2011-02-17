require 'rbbt/util/misc'
require 'rbbt/util/tsv'

module Path
  attr_accessor :pkg_module, :datadir

  def self.find_files_back_to(path, target, subdir)
    return [] if path.nil?
    raise "Path #{ path } not in directory #{ subdir }" if not Misc.in_directory? path, subdir

    pkg_module = path.pkg_module

    files = []
    while path != subdir
      path = File.dirname(path)
      path.extend Path
      path.pkg_module = pkg_module
      path.datadir    = path.datadir
      if path[target].exists? 
        files << path[target]
      end
    end

    files
  end

  def self.path(string, datadir = nil, pkg_module = nil)
    string.extend Path
    string.datadir = datadir
    string.pkg_module = case
                        when pkg_module.nil?
                          nil
                        when String === pkg_module
                          Misc.string2const pkg_module
                        else
                          pkg_module
                        end
    string
  end

  def method_missing(name, *args, &block)
    new = File.join(self.dup, name.to_s)
    new.extend Path
    new.pkg_module = pkg_module
    new.datadir    = datadir
    new
  end

  def [](name)
    new = File.join(self.dup, name.to_s)
    new.extend Path
    new.pkg_module = pkg_module
    new.datadir    = datadir
    new
  end

  def namespace
    return nil if self.nil? or self.empty? or (not datadir.nil? and (self == datadir or File.dirname(self) == datadir))
    if File.directory? self
      File.basename(self)
    else
      File.basename(File.dirname(self))
    end
  end

  def identifier_files
    if datadir.nil?
      path = File.join(File.dirname(self), 'identifiers')
      path.extend Path
      path.pkg_module = pkg_module
      if path.exists?
        [path]
      else
        []
      end
    else
      identifier_files = Path.find_files_back_to(self, 'identifiers', datadir)
      return identifier_files.collect{|f| Path.path(f, datadir, pkg_module)}
    end
  end

  def tsv(key = nil, options = {})
    if options.empty? and Hash === key
      options, key = key, nil
    end

    produce
    #TSV.new self, key, options.merge(:datadir => datadir, :namespace => namespace)
    TSV.new self, key, options
  end

  def index(options = {})
    produce
    TSV.index self, options
  end

  def open(options = {})
    produce
    Open.open(self, options)
  end

  def read(options = {})
    produce
    Open.read(self, options)
  end

  def fields(sep = nil, header_hash = nil)
    produce
    TSV.parse_header(self.open, sep, header_hash)[1].collect{|f| f.extend TSV::Field; f.namespace = namespace ;f}
  end

  def all_fields(sep = nil, header_hash = nil)
    produce
    key_field, fields = TSV.parse_header(self.open, sep, header_hash).values_at(0, 1).flatten.collect{|f| f.extend TSV::Field; f.namespace = namespace; f}
  end

  def fields_in_namespace(sep = nil, header_hash = nil)
    produce
    TSV.parse_header(self.open, sep, header_hash)[1].collect{|f| f.extend TSV::Field; f.namespace = namespace ;f}.select{|f| f.namespace == namespace}
  end

  def all_namespace_fields(namespace, sep = /\t/, header_hash = "#")
    produce
    key_field, fields = TSV.parse_header(self.open, sep, header_hash).values_at(0, 1).flatten.collect{|f| f.extend TSV::Field; f.namespace = namespace; f}.select{|f| f.namespace == namespace}
  end

  def filename
    self.to_s
  end 

  def exists?
    begin
      produce
      true
    rescue
      false
    end
  end

  def produce
    return self if File.exists? self

    raise "No especified pkg_data for file #{ self }, cannot produce" if pkg_data.nil?

    Log.debug("Trying to produce '#{ self }'")
    file, producer = pkg_module.reclaim self

    raise "File #{self} has not been claimed, cannot produce" if file.nil? or producer.nil?

    pkg_module.produce(self, producer[:get], producer[:subdir], producer[:sharedir])

    self
  end
end


