require 'rbbt-util'

module Persist
  class Sharder
    attr_accessor :directory, :params, :shard_function, :databases, :closed, :writable, :mutex

    def initialize(directory, *rest, &block)
      @shard_function = block
      @params = rest
      @databases = {}
      @directory = directory
      @mutex = Mutex.new
    end

    def database(key)
      shard = shard_function.call(key)
      databases[shard] ||= begin
                             path = File.join(directory, 'shard-' << shard.to_s)
                             Persist.open_database(path, *params)
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
      databases.each do |database|
        database.each do |k,v|
          yield k, v
        end
      end
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
      lock_filename = Persist.persistence_path(File.join(directory, 'write'), {:dir => TSV.lock_dir})
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
      lock_filename = Persist.persistence_path(File.join(directory, 'write'), {:dir => TSV.lock_dir})
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
      databases.values.collect{|d| d.keys }.flatten
    end

    def []=(key, value)
      database(key)[key] = value
    end

    def [](key, value)
      database(key)[key]
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
end
