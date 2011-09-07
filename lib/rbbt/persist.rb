require 'digest/md5'

require 'rbbt/util/misc'
require 'rbbt/util/open'

require 'rbbt/persist/tsv'

module Persist
  CACHEDIR="/tmp/tsv_persistent_cache"
  FileUtils.mkdir CACHEDIR unless File.exist? CACHEDIR

  def self.cachedir=(cachedir)
    CACHEDIR.replace cachedir
    FileUtils.mkdir_p CACHEDIR unless File.exist? CACHEDIR
  end

  def self.cachedir
    CACHEDIR
  end
 
  def self.newer?(path, file)
    return true if not File.exists? file
    return true if File.mtime(path) < File.mtime(file)
    return false
  end

  def self.is_persisted?(path, persist_options = {})
    return false if not File.exists? path
    return false if TrueClass === persist_options[:update]

    check = persist_options[:check]
    if not check.nil?
      if Array === check
        return false if check.select{|file| newer? path, file}.any?
     else
        return false if newer? path, check
     end
    end

    return true
  end

  def self.persistence_path(file, persist_options = {}, options = {})
    persistence_file = Misc.process_options persist_options, :file
    return persistence_file if not persistence_file.nil?

    prefix = Misc.process_options persist_options, :prefix

    if prefix.nil?
      perfile = file.gsub(/\//, '>') 
    else
      perfile = prefix.to_s + ":" + file.gsub(/\//, '>') 
    end

    if options.include? :filters
      options[:filters].each do |match,value|
        perfile = perfile + "&F[#{match}=#{Misc.digest(value.inspect)}]"
      end
    end

    persistence_dir = Misc.process_options(persist_options, :dir) || CACHEDIR

    filename = perfile.gsub(/\s/,'_').gsub(/\//,'>')
    options_md5 = Misc.hash2md5 options
    filename  << ":" << options_md5 unless options_md5.empty?

    File.join(persistence_dir, filename)
  end

  def self.load_file(path, type)
    case (type || "nil").to_sym
    when :nil
      nil
    when :tsv
      TSV.open(path)
    when :marshal_tsv
      TSV.setup(Marshal.load(Open.open(path)))
    when :fwt
      FixWidthTable.get(path) 
    when :string, :text
      Open.read(path)
    when :array
      res = Open.read(path).split("\n", -1)
      res.pop
      res
    when :marshal
      Marshal.load(Open.open(path))
    when :yaml
      YAML.load(Open.open(path))
    when :float
      Open.read(path).to_f
    when :integer
      Open.read(path).to_i
    when :tsv
      TSV.open(Open.open(path))
    else
      raise "Unknown persistence: #{ type }"
    end
  end

  def self.save_file(path, type, content)

    return if (content.nil? and File.exists? path)
    
    case (type || "nil").to_sym
    when :nil
      nil
    when :fwt
      content.file.seek 0
      Open.write(path, content.file.read)
    when :tsv
      Open.write(path, content.to_s)
    when :string, :text
      Open.write(path, content)
    when :array
      Open.write(path, content * "\n" + "\n")
    when :marshal_tsv
      Open.write(path, Marshal.dump(content.dup))
    when :marshal
      Open.write(path, Marshal.dump(content))
    when :yaml
      Open.write(path, YAML.dump(content))
    when :float, :integer, :tsv
      Open.write(path, content.to_s)
    else
      raise "Unknown persistence: #{ type }"
    end
  end

  def self.persist(name, type = nil, persist_options = {})
    type ||= :marshal
    persist_options = Misc.add_defaults persist_options, :persist => true

    if persist_options[:persist]
      path = persistence_path(name, persist_options)
      Misc.lock(path) do
        if is_persisted?(path, persist_options)
          Log.debug "Persist up-to-date: #{ path } - #{persist_options.inspect}"
          return load_file(path, type) 
        else
          Log.debug "Persist create: #{ path } - #{persist_options.inspect}"
        end
        res = yield
        save_file(path, type, res)
        res
      end
    else
      yield
    end
  end
end

module LocalPersist

  attr_accessor :local_persist_dir
  def local_persist(name, type = nil, options= {}, &block)
    Persist.persist(name, type, options.merge({:dir => @local_persist_dir}), &block)
  end

  def local_persist_tsv(source, name, opt = {}, options= {}, &block)
    Persist.persist_tsv(source, name, opt, options.merge({:dir => @local_persist_dir}), &block)
  end

end
