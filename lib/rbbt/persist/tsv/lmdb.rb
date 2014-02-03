require 'lmdb'

module Persist

  module LMDBAdapter
    attr_accessor :persistence_path, :closed

    def self.open(path, write)

      database = CONNECTIONS[path] ||= begin
                                         dir = File.dirname(File.expand_path(path))
                                         file = File.basename(path)
                                         env = LMDB.new(dir, :mapsize => 1024 * 10000)
                                         database = env.database file, :create => write
                                         database
                                       end

      database.extend Persist::LMDBAdapter unless Persist::LMDBAdapter === database
      database.persistence_path ||= path

      database
    end

    def keys
      keys = []
      cursor do |cursor|
        while p = cursor.next
          keys << p.first
        end
      end
      keys
    end
    
    def include?(key)
      self.send(:[], key, true)
    end

    def closed?
      false
    end

    def close
      self
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

    def each
      cursor do |cursor|
        while pair = cursor.next
          yield *pair
        end
      end
      self
    end

    def collect
      res = []
      cursor do |cursor|
        while pair = cursor.next
          res = if block_given?
                  yield *pair
                else
                  pair
                end
        end
      end
      res
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


  def self.open_lmdb(path, write, serializer = nil)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::LMDBAdapter.open(path, write)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
