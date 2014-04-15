require 'rbbt'
require 'rbbt/util/misc'
require 'rbbt/util/open'

require 'rbbt/persist/tsv'
require 'set'

module Persist
  class << self
    attr_accessor :cachedir
    def self.cachedir=(cachedir)
      @cachedir = Path === cachedir ? cachedir : Path.setup(cachedir)
    end
    def self.cachedir
      @cachedir ||= Rbbt.var.cache.persistence 
    end

    attr_accessor :lock_dir
    
    def lock_dir
      @lock_dir ||= Rbbt.tmp.tsv_open_locks.find
    end
  end

  MEMORY = {} unless defined? MEMORY
  MAX_FILE_LENGTH = 150

  def self.newer?(path, file)
    return true if not Open.exists? file
    return true if File.mtime(path) < File.mtime(file)
    return false
  end

  def self.is_persisted?(path, persist_options = {})
    return false if not Open.exists? path
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
    return persistence_file unless persistence_file.nil?

    prefix = Misc.process_options persist_options, :prefix

    if prefix.nil?
      perfile = file.to_s.gsub(/\//, '>') 
    else
      perfile = prefix.to_s + ":" + file.to_s.gsub(/\//, '>') 
    end

    if options.include? :filters
      options[:filters].each do |match,value|
        perfile = perfile + "&F[#{match}=#{Misc.digest(value.inspect)}]"
      end
    end

    persistence_dir = Misc.process_options(persist_options, :dir) || Persist.cachedir 
    Path.setup(persistence_dir) unless Path === persistence_dir

    filename = perfile.gsub(/\s/,'_').gsub(/\//,'>')
    clean_options = options
    clean_options.delete :unnamed
    clean_options.delete "unnamed"

    filename = filename[0..MAX_FILE_LENGTH] << Misc.digest(filename[MAX_FILE_LENGTH+1..-1]) if filename.length > MAX_FILE_LENGTH + 10

    options_md5 = Misc.hash2md5 clean_options
    filename  << ":" << options_md5 unless options_md5.empty?

    persistence_dir[filename].find
  end

  TRUE_STRINGS = Set.new ["true", "True", "TRUE", "t", "T", "1", "yes", "Yes", "YES", "y", "Y", "ON", "on"] unless defined? TRUE_STRINGS
  def self.load_file(path, type)
    begin
      case (type || :marshal).to_sym
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
      when :binary
        f = File.open(path, 'rb')
        res = f.read
        f.close
        res.force_encoding("ASCII-8BIT") if res.respond_to? :force_encoding
        res
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
      else
        raise "Unknown persistence: #{ type }"
      end
    rescue
      Log.error "Exception loading #{ type } #{ path }: #{$!.message}"
      raise $!
    end
  end

  def self.save_file(path, type, content)

    return if content.nil?

    case (type || :marshal).to_sym
    when :nil
      nil
    when :boolean
      Misc.sensiblewrite(path, content ? "true" : "false")
    when :fwt
      content.file.seek 0
      Misc.sensiblewrite(path, content.file.read)
    when :tsv
      content = content.to_s if TSV === content
      Misc.sensiblewrite(path, content)
    when :annotations
      Misc.sensiblewrite(path, Annotated.tsv(content, :all).to_s)
    when :string, :text
      Misc.sensiblewrite(path, content)
    when :binary
      content.force_encoding("ASCII-8BIT") if content.respond_to? :force_encoding
      f = File.open(path, 'wb')
      f.puts content
      f.close
      content
    when :array
      case content
      when Array
        if content.empty?
          Misc.sensiblewrite(path, "")
        else
          Misc.sensiblewrite(path, content * "\n" + "\n")
        end
      when IO
        Misc.sensiblewrite(path, content)
      else
        Misc.sensiblewrite(path, content.to_s)
      end
    when :marshal_tsv
      Misc.sensiblewrite(path, Marshal.dump(content.dup))
    when :marshal
      Misc.sensiblewrite(path, Marshal.dump(content))
    when :yaml
      Misc.sensiblewrite(path, YAML.dump(content))
    when :float, :integer, :tsv
      Misc.sensiblewrite(path, content.to_s)
    else
      raise "Unknown persistence: #{ type }"
    end
  end

  #def self.tee_stream_fork(stream, path, type, callback = nil)
  #  file, out = Misc.tee_stream(stream)

  #  saver_pid = Process.fork do
  #    out.close
  #    stream.close
  #    Misc.purge_pipes
  #    begin
  #      Misc.lock(path) do
  #        save_file(path, type, file)
  #      end
  #    rescue Aborted
  #      stream.abort if stream.respond_to? :abort
  #      raise $!
  #    rescue Exception
  #      Log.exception $!
  #      Kernel.exit! -1
  #    end
  #    Kernel.exit! 0
  #  end
  #  file.close
  #  ConcurrentStream.setup(out, :pids => [saver_pid], :filename => path)
  #end

  def self.tee_stream_thread(stream, path, type, callback = nil)
    file, out = Misc.tee_stream(stream)

    saver_thread = Thread.new(Thread.current, path, file) do |parent,path,file|
      begin
        Thread.current["name"] = "file saver: " + path
        Misc.lock(path) do
          save_file(path, type, file)
        end
      rescue Aborted
        Log.error "Persist stream thread aborted: #{ Log.color :blue, path }"
        stream.abort if stream.respond_to? :abort
      rescue Exception
        Log.error "Persist stream thread exception: #{ Log.color :blue, path }"
        Log.exception $!
        stream.abort if stream.respond_to? :abort
        parent.raise $!
      end
    end
    ConcurrentStream.setup(out, :threads => saver_thread, :filename => path)
  end

  def self.tee_stream_pipe(stream, path, type, callback = nil)
    parent_pid = Process.pid
    out = Misc.open_pipe true, false do |sin|
      begin
        file, out = Misc.tee_stream(stream)

        saver_th = Thread.new(Thread.current, path, file) do |parent,path,file|
          begin
            Misc.lock(path) do
              save_file(path, type, file)
            end
            Log.high "Stream pipe saved: #{path}"
          rescue Aborted
            Log.error "Persist stream pipe exception: #{ Log.color :blue, path }"
            stream.abort if stream.respond_to? :abort
          rescue Exception
            Log.error "Persist stream pipe exception: #{ Log.color :blue, path }"
            Log.exception $!
            stream.abort if stream.respond_to? :abort
            parent.raise $!
          end
        end

        tee_th = Thread.new(Thread.current) do |parent|
          begin
            while block = out.read(2028)
              sin.write block
            end
          rescue Aborted
            Log.error "Tee stream thread aborted"
            sout.abort if sout.respond_to? :abort
            sin.abort if sin.respond_to? :abort
          rescue Exception
            sin.abort if sin.respond_to? :abort
            sout.abort if sout.respond_to? :abort
            Log.exception $!
            parent.raise $!
          ensure
            sin.close unless sin.closed?
          end
        end
        saver_th.join
        tee_th.join
      rescue Aborted
        tee_th.raise Aborted.new if tee_th and tee_th.alive?
        saver_th.raise Aborted.new if saver_th and saver_th.alive?
        Kernel.exit! -1
      rescue Exception
        tee_th.raise Aborted.new if tee_th and tee_th.alive?
        saver_th.raise Aborted.new if saver_th and saver_th.alive?
        Log.exception $!
        Process.kill :INT, parent_pid
        Kernel.exit! -1
      end
    end
    stream.close
    out
  end

  class << self
    alias tee_stream tee_stream_thread 
  end


  def self.persist(name, type = nil, persist_options = {})
    type ||= :marshal

    return (persist_options[:repo] || Persist::MEMORY)[persist_options[:file]] ||= yield if type ==:memory and persist_options[:file] and persist_options[:persist] and persist_options[:persist] != :update

    if FalseClass == persist_options[:persist]
      yield
    else
      other_options = Misc.process_options persist_options, :other
      path = persistence_path(name, persist_options, other_options || {})

      case 
      when type.to_sym == :memory
        repo = persist_options[:repo] || Persist::MEMORY
        repo[path] ||= yield

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
        when (keys.any? and not keys.first =~ /ANNOTATED_DOUBLE_ARRAY/)
          repo.read_and_close do
            keys.sort_by{|k| k.split(":").last.to_i}.collect{|key|
              v = repo[key]
              Annotated.load_tsv_values(key, v, "literal", "annotation_types", "JSON")
            }
          end
        when (keys.any? and keys.first =~ /ANNOTATED_DOUBLE_ARRAY/)
          repo.read_and_close do

            res = keys.sort_by{|k| k.split(":").last.to_i}.collect{|key|
              v = repo[key]
              Annotated.load_tsv_values(key, v, "literal", "annotation_types", "JSON")
            }

            res.first.annotate res
            res.extend AnnotatedArray

            res
          end
        else
          entities = yield

          repo.write_and_close do 
            case
            when entities.nil?
              repo[subkey + "NIL"] = nil
            when entities.empty?
              repo[subkey + "EMPTY"] = nil
            when (not Array === entities or (AnnotatedArray === entities and not Array === entities.first))
              tsv_values = entities.tsv_values("literal", "annotation_types", "JSON") 
              repo[subkey + entities.id << ":" << "SINGLE"] = tsv_values
            when (not Array === entities or (AnnotatedArray === entities and AnnotatedArray === entities.first))
              entities.each_with_index do |e,i|
                next if e.nil?
                tsv_values = e.tsv_values("literal", "annotation_types", "JSON") 
                repo[subkey + e.id << ":ANNOTATED_DOUBLE_ARRAY:" << i.to_s] = tsv_values
              end
            else
              entities.each_with_index do |e,i|
                next if e.nil?
                tsv_values = e.tsv_values("literal", "annotation_types", "JSON") 
                repo[subkey + e.id << ":" << i.to_s] = tsv_values
              end
            end
          end

          entities
        end

      else

        if is_persisted?(path, persist_options)
          Log.low "Persist up-to-date: #{ path } - #{Misc.fingerprint persist_options}"
          return path if persist_options[:no_load]
          return load_file(path, type) 
        end

        begin

          lock_filename = Persist.persistence_path(path + '.persist', {:dir => Persist.lock_dir})
          Misc.lock lock_filename  do |lockfile|

            if is_persisted?(path, persist_options)
              Log.low "Persist up-to-date (suddenly): #{ path } - #{Misc.fingerprint persist_options}"
              return path if persist_options[:no_load]
              return load_file(path, type) 
            end

            Log.medium "Persist create: #{ path } - #{Misc.fingerprint persist_options}"

            res = yield

            if persist_options[:no_load] == :stream 
              case res
              when IO
                res = tee_stream(res, path, type, res.respond_to?(:callback)? res.callback : nil)
                ConcurrentStream.setup res do
                  begin
                    lockfile.unlock if lockfile.locked?
                  rescue
                    Log.exception $!
                    Log.warn "Lockfile exception: " << $!.message
                  end
                end
                res.abort_callback = Proc.new do
                  begin
                    lockfile.unlock if lockfile.locked?
                  rescue
                    Log.exception $!
                    Log.warn "Lockfile exception: " << $!.message
                  end
                end
                raise KeepLocked.new res 
              when TSV::Dumper
                res = tee_stream(res.stream, path, type, res.respond_to?(:callback)? res.callback : nil)
                ConcurrentStream.setup res do
                  begin
                    lockfile.unlock
                  rescue
                    Log.exception $!
                    Log.warn "Lockfile exception: " << $!.message
                  end
                end
                res.abort_callback = Proc.new do
                  begin
                    lockfile.unlock
                  rescue
                    Log.exception $!
                    Log.warn "Lockfile exception: " << $!.message
                  end
                end
                raise KeepLocked.new res 
              end
            end

            case res
            when IO
              begin
                res = case
                      when :array
                        res.read.split "\n"
                      when :tsv
                        TSV.open(res)
                      else
                        res.read
                      end
                res.join if res.respond_to? :join
              rescue
                res.abort if res.respond_to? :abort
                raise $!
              end
            when (defined? TSV and TSV::Dumper)
              begin
                io = res.stream
                res = TSV.open(io)
                io.join if io.respond_to? :join
              rescue
                io.abort if io.respond_to? :abort
                raise $!
              end
            end

            Misc.lock(path) do
              save_file(path, type, res)
            end

            persist_options[:no_load] ? path : res
          end

        rescue
          Log.error "Error in persist: #{path}#{Open.exists?(path) ? Log.color(:red, " Erasing") : ""}"
          FileUtils.rm path if Open.exists? path 
          raise $!
        end
      end

    end
  end

  def self.memory(name, options = {}, &block)
    case options
    when nil
      persist name, :memory, :file => name, &block
    when String
      persist name, :memory, :file => name + "_" << options, &block
    else
      file = name
      repo = options.delete :repo if options and options.any?
      file << "_" << (options[:key] ? options[:key] : Misc.hash2md5(options)) if options and options.any?
      persist name, :memory, {:repo => repo, :persist => true, :file => file}.merge(options), &block
    end
  end
end

module LocalPersist

  attr_accessor :local_persist_dir

  def local_persist_dir
    @local_persist_dir ||= Rbbt.var.cache.persistence if defined? Rbbt
    @local_persist_dir
  end

  def local_persist(name, type = nil, options= {}, persist_options = nil, &block)
    persist_options ||= {}
    persist_options = {:dir => local_persist_dir}.merge persist_options
    persist_options[:other] = options
    Persist.persist(name, type, persist_options, &block)
  end

  def local_persist_tsv(source, name, opt = {}, options= {}, &block)
    Persist.persist_tsv(source, name, opt, options.merge({:dir => local_persist_dir, :persist => true}), &block)
  end
end
