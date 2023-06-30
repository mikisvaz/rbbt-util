module Path

  #def _exists?
  #  Open.exists? self.find
  #end

  #def exists?
  #  begin
  #    self.produce
  #    _exists?
  #  rescue Exception
  #    false
  #  end
  #end

  #def open(options = {}, &block)
  #  file = Open.remote?(self) || Open.ssh?(self) ? self : self.produce.find
  #  Open.open(file, options, &block)
  #end

  #def read(&block)
  #  Open.read(self.produce.find, &block)
  #end

  #def write(*args, &block)
  #  Open.write(self.find(:default), *args, &block)
  #end

  #def index(options = {})
  #  TSV.index(self.produce, **options)
  #end

  def basename
    Path.setup(File.basename(self), self.resource, self.pkgdir)
  end

  #def tsv(*args)
  #  begin
  #    path = self.produce
  #  rescue ResourceNotFound => e
  #    begin
  #      path = self.set_extension('tsv').produce
  #    rescue ResourceNotFound 
  #      raise e
  #    end
  #  end
  #  TSV.open(path, *args)
  #end

  def tsv_options(options = {})
    self.open do |stream|
      TSV::Parser.new(stream, options).options
    end
  end

  def traverse(options = {}, &block)
    TSV::Parser.traverse(self.open, options, &block)
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
      header = TSV.parse_header(stream)
      [header.key_field] + header.fields
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


end
