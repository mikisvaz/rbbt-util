module Persist
  module SharderAdapter
    def self.open(path, write, type=nil, &block)

      database = CONNECTIONS[path] ||= Sharder.new(path, write, type, &block)

      database.extend Persist::SharderAdapter unless Persist::SharderAdapter === database

      database
    end

  end

  class Sharder
    attr_accessor :persistence_path, :shard_function, :databases, :closed, :writable, :mutex, :db_type

    def initialize(persistence_path, write = false, db_type=nil, &block)
      @shard_function = block
      @persistence_path = Path.setup(persistence_path)
      @mutex = Mutex.new
      @writable = write
      @db_type = db_type

      if write
        @databases = {} 
      end
    end

    def <<(key,value)
      self[key] = value
    end

    def persistence_path=(path)
      @persistence_path = path
    end

    def databases
      @databases ||= begin
                       hash = {}
                       @persistence_path.glob('shard-*').each do |f|
                         shard = File.basename(f).match(/shard-(.*)/)[1]
                         hash[shard] = Persist.open_database(f, false, :clean, db_type)
                       end
                       hash
                     end
    end

    def database(key)
      shard = key =~ /__tsv_/ ? "0" : shard_function.call(key)
      if databases.include? shard
        databases[shard]
      else
        database ||= begin
                       path = File.join(persistence_path, 'shard-' << shard.to_s)
                       (writable or File.exists?(path)) ? Persist.open_database(path, writable, :clean, db_type) : nil
                     end
        if database
          databases[shard] = database 
        else
          Log.warn "Database #{ path } missing" if
          nil
        end
      end
    end

    MAX_CHAR = 255.chr

    def prefix(key)
      range(key, 1, key + MAX_CHAR, 1)
    end

    def get_prefix(key)
      keys = prefix(key)
      select(:key => keys)
    end

    def closed?
      @closed
    end

    def close
      @closed = true
      super
    end

    def read(force = false)
      return if not write? and not closed and not force
      self.close
      databases.each{|d| d.read }
      @writable = false
      @closed = false
      self
    end

    def write(force = true)
      return if write? and not closed and not force
      self.close

      databases.each{|d| d.write }

      @writable = true
      @closed = false
      self
    end

    def write?
      @writable
    end

    def read?
      ! write?
    end

    def each
      databases.values.each do |database|
        database.each do |k,v|
          yield k, v
        end
      end
    end

    def include?(key)
      self[key] != nil
    end

    def collect
      res = []
      each do |key, value|
        res << if block_given?
                 yield key, value
        else
          [key, value]
        end
      end
      res
    end

    def write_and_read
      lock_filename = Persist.persistence_path(File.join(persistence_path, 'write'), {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        @mutex.synchronize do
          write if @closed or not write?
          res = begin
                  yield
                ensure
                  read
                end
          res
        end
      end
    end

    def write_and_close
      lock_filename = Persist.persistence_path(File.join(persistence_path, 'write'), {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        @mutex.synchronize do
          write if @closed or not write?
          res = begin
                  yield
                ensure
                  close
                end
          res
        end
      end
    end

    def read_and_close
      @mutex.synchronize do
        read if @closed or not read?
        res = begin
                yield
              ensure
                close
              end
        res
      end
    end

    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end

    def keys
      databases.values.collect{|d| d.keys }.flatten - TSV::ENTRY_KEYS.to_a
    end

    def []=(key, value, clean = false)
      database(key).send(:[]=, key, value)
    end

    def [](key, clean=false)
      v = database(key).send(:[], key)
    end

    def <<(p)
      return if p.nil?
      self[p.first] = p.last
    end

    def write
      databases.values.each{|database| database.write }
    end

    def read
      databases.values.each{|database| database.read }
    end

    def close
      databases.values.each{|database| database.close }
    end
  end

  def self.open_sharder(path, write, serializer = nil, tokyocabinet_class = TokyoCabinet::HDB, &shard_function)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::SharderAdapter.open(path, write, tokyocabinet_class, &shard_function)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer if serializer
    end

    database
  end
end
