require 'leveldb'

module Persist

  module LevelDBAdapter
    attr_accessor :persistence_path, :closed, :writable

    def self.open(path, write)

      database = CONNECTIONS[path] ||= begin
                                         LevelDB::DB.new path
                                       end

      database.extend Persist::LevelDBAdapter unless Persist::LevelDBAdapter === database
      database.persistence_path ||= path

      database
    end

    def prefix(key)
      range(key, 1, key + 255.chr, 1)
    end

    def get_prefix(key)
      keys = prefix(key)
      select(:key => keys)
    end

    def include?(key)
      includes?(key)
    end

    def closed?
      @closed
    end

    def close
      @closed = true
    end

    def read(force = false)
      self
    end

    def write(force = true)
      self
    end

    def write?
      @writable
    end

    def delete(key)
      out(key)
    end

    def write_and_read
      lock_filename = Persist.persistence_path(persistence_path, {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        write if @closed or not write?
        res = begin
                yield
              ensure
                read
              end
        res
      end
    end

    def write_and_close
      lock_filename = Persist.persistence_path(persistence_path, {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        write if @closed or not write?
        res = begin
                yield
              ensure
                close
              end
        res
      end
    end

    def read_and_close
      read if @closed or write?
      res = begin
              yield
            ensure
              close
            end
      res
    end

    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end


    def range(*args)
      super(*args) - TSV::ENTRY_KEYS.to_a
    end
  end


  def self.open_leveldb(path, write, serializer = nil)
    write = true unless File.exist? path

    FileUtils.mkdir_p File.dirname(path) unless File.exist?(File.dirname(path))

    database = Persist::LevelDBAdapter.open(path, write)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
