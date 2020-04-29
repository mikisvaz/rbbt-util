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
      ! (write? || closed?)
    end

    def write(*args)
      begin
        super(*args)
        @writable = true
      rescue NoMethodError
      end
    end

    def close(*args)
      begin
        super(*args)
      rescue NoMethodError
      end
    end

    def read(*args)
      begin
        super(*args)
      rescue NoMethodError
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

    def delete(key)
      out(key)
    end

    def lock
      #return yield if @locked
      lock_filename = Persist.persistence_path(persistence_path, {:dir => TSV.lock_dir})
      Misc.lock(lock_filename) do
        begin
          @locked = true
          yield
        ensure
          @locked = false
        end
      end
    end

    def write_and_read
      if write?
        begin
          return yield
        ensure
          read
        end
      end

      lock do
        write true if closed? or not write?
        begin
          yield
        ensure
          read
        end
      end
    end

    def write_and_close
      if write?
        begin
          return yield
        ensure
          close
        end
      end

      lock do
        write true if closed? || ! write?
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
    end

    def read_and_close
      if read?
        begin
          return yield
        ensure
          close
        end
      end

      lock do
        read true if closed? || ! read?
        begin
          yield
        ensure
          close
        end
      end
    end


    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end


    def range(*args)
      super(*args) #- TSV::ENTRY_KEYS.to_a
    end

    def include?(*args)
      read if closed?
      super(*args) #- TSV::ENTRY_KEYS.to_a
    end
  end
end
