require 'rbbt/tsv'
module Persist
  module TSVAdapter
    attr_accessor :persistence_path, :closed, :writable, :mutex

    MAX_CHAR = 255.chr

    def mutex
      @mutex ||= Mutex.new
    end

    def closed?
      @closed
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
        @closed = true
      rescue NoMethodError
      end
      self
    end

    def read(*args)
      begin
        super(*args)
      rescue NoMethodError
      end
    end

    def delete(key)
      out(key)
    end

    def lock
      return yield if @locked
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

    def lock_and_close
      lock do
        begin
          yield
        ensure
          close
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
        write(true) if closed? || !write?
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
          close unless @locked
        end
      end

      lock do
        write(true) if closed? || ! write?
        res = begin
                yield
              ensure
                close
              end
        res
      end
    end

    def read_and_close
      if read? || write?
        begin
          return yield
        ensure
          close unless @locked
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

    def read_lock
      read if closed?
      if read? || write?
        return yield
      end

      lock do
        close
        read true
        begin
          yield
        end
      end
    end

    def write_lock
      write if closed?
      if write?
        return yield
      end

      lock do
        close
        write true
        begin
          yield
        end
      end
    end


    def merge!(hash)
      hash.each do |key,values|
        self[key] = values
      end
    end

    def range(*args)
      self.read_lock do
        super(*args)
      end
    end

    def include?(*args)
      self.read_lock do
        super(*args) #- TSV::ENTRY_KEYS.to_a
      end
    end

    def [](*args)
      self.read_lock do
        super(*args) #- TSV::ENTRY_KEYS.to_a
      end
    end

    def []=(*args)
      self.write_lock do
        super(*args) #- TSV::ENTRY_KEYS.to_a
      end
    end

    def keys(*args)
      self.read_lock do
        super(*args)
      end
    end


    def prefix(key)
      self.read_lock do
        range(key, 1, key + MAX_CHAR, 1)
      end
    end

    def get_prefix(key)
      keys = prefix(key)
      select(:key => keys)
    end


    def size(*args)
      self.read_lock do
        super(*args)
      end
    end

    def each(*args, &block)
      self.read_lock do
        super(*args, &block)
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

    def values_at(*keys)
      self.read_lock do
        keys.collect do |k|
          self[k]
        end
      end
    end
  end
end
