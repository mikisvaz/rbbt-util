require 'digest/md5'

require 'rbbt/util/misc'
require 'rbbt/util/open'

require 'rbbt/persist/tsv'
require 'set'

module Persist
  CACHEDIR="/tmp/tsv_persistent_cache"
  FileUtils.mkdir CACHEDIR unless File.exist? CACHEDIR

  MEMORY = {}

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
    clean_options = options
    clean_options.delete :unnamed
    clean_options.delete "unnamed"

    options_md5 = Misc.hash2md5 clean_options
    filename  << ":" << options_md5 unless options_md5.empty?

    File.join(persistence_dir, filename)
  end

  TRUE_STRINGS = Set.new ["true", "True", "TRUE", "t", "T", "1", "yes", "Yes", "YES", "y", "Y", "ON", "on"]
  def self.load_file(path, type)
    case (type || "nil").to_sym
    when :nil
      nil
    when :boolean
      TRUE_STRINGS.include? Open.read(path).chomp.strip
    when :annotations
      Annotated.load_tsv TSV.open(path)
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

    return if content.nil?
    
    case (type || "nil").to_sym
    when :nil
      nil
    when :boolean
      Open.write(path, content ? "true" : "false")
    when :fwt
      content.file.seek 0
      Open.write(path, content.file.read)
    when :tsv
      Open.write(path, content.to_s)
    when :annotations
      Open.write(path, Annotated.tsv(content, :all).to_s)
    when :string, :text
      Open.write(path, content)
    when :array
      if content.empty?
        Open.write(path, "")
      else
        Open.write(path, content * "\n" + "\n")
      end
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
    other_options = Misc.process_options persist_options, :other

    if persist_options[:persist]
      path = persistence_path(name, persist_options, other_options || {})

      case 
      when type.to_sym === :memory
        Persist::MEMORY[path] ||= yield

      when (type.to_sym == :annotations and persist_options.include? :annotation_repo)

        repo = persist_options[:annotation_repo]

        keys = nil
        subkey = name + ":"

        if String === repo
          repo = Persist.open_tokyocabinet(repo, false, :list, "BDB")
          repo.read_and_close do
            keys = repo.range subkey + 0.chr, true, subkey + 254.chr, true
          end
          repo.close
        else
          repo.read_and_close do
            keys = repo.range subkey + 0.chr, true, subkey + 254.chr, true
          end
        end

        case
        when (keys.length == 1 and keys.first == subkey + 'NIL')
          nil
        when (keys.length == 1 and keys.first == subkey + 'EMPTY')
          []
        when (keys.length == 1 and keys.first =~ /:SINGLE$/)
          key = keys.first
          values = repo.read_and_close do
            repo[key]
          end
          Annotated.load_tsv_values(key, values, "literal", "annotation_types", "JSON")
        when keys.any?
          repo.read_and_close do
            keys.collect{|key|
              v = repo[key]
              Annotated.load_tsv_values(key, v, "literal", "annotation_types", "JSON")
            }
          end
        else
          entities = yield

          Misc.lock(repo.persistence_path) do
            repo.write_and_close do 
              case
              when entities.nil?
                repo[subkey + "NIL"] = nil
              when entities.empty?
                repo[subkey + "EMPTY"] = nil
              when (not Array === entities or AnnotatedArray === entities)
                tsv_values = entities.tsv_values("literal", "annotation_types", "JSON") 
                repo[subkey + entities.id << ":" << "SINGLE"] = tsv_values
              else
                entities.each do |e|
                  tsv_values = e.tsv_values("literal", "annotation_types", "JSON") 
                  repo[subkey + e.id] = tsv_values
                end
              end
            end
          end

          entities
        end

      else
        Misc.lock(path) do
          if is_persisted?(path, persist_options)
            Log.debug "Persist up-to-date: #{ path } - #{persist_options.inspect[0..100]}"
            return nil if persist_options[:no_load]
            return load_file(path, type) 
          else
            Log.debug "Persist create: #{ path } - #{persist_options.inspect[0..100]}"
          end
          begin
            res = yield
            save_file(path, type, res)
            res
          rescue
            Log.high "Error in persist. Erasing '#{ path }'"
            FileUtils.rm path if File.exists? path
            raise $!
          end
        end
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
