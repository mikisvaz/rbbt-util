require 'rbbt/tsv'
module Persist
  module TSVAdapter
    attr_accessor :persistence_path, :closed, :writable, :mutex

    MAX_CHAR = 255.chr

    def mutex
      @mutex ||= Mutex.new
    end

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
      self
    end

    def write?
      @writable
    end

    def read?
      ! write?
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

    def delete(key)
      out(key)
    end

    def write_and_read
      lock_filename = Persist.persistence_path(persistence_path + '.write', {:dir => TSV.lock_dir})
      #mutex.synchronize do
        Misc.lock(lock_filename) do
          write if closed? or not write?
          res = begin
                  yield
                ensure
                  read
                end
          res
        end
      #end
    end

    def write_and_close
      lock_filename = Persist.persistence_path(persistence_path + '.write', {:dir => TSV.lock_dir})
      #mutex.synchronize do
        Misc.lock(lock_filename, true) do
          write if closed? or not write?
          res = begin
                  yield
                rescue Exception
                  Log.exception $!
                  raise $!
                ensure
                  close
                end
          res
        end
      #end
    end

    def read_and_close
      #mutex.synchronize do
      read if closed? or not read?
      res = begin
              yield
            ensure
              close
            end
      res
      #end
    end


    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end


    def range(*args)
      super(*args) #- TSV::ENTRY_KEYS.to_a
    end
  end
end
