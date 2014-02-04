require 'libcdb'

module Persist

  module CDBAdapter
    attr_accessor :persistence_path, :closed

    def self.open(path, write)
      write = true unless File.exists? path

     database = CONNECTIONS[path] ||= begin
                                         file = File.open(path, 'w')
                                         LibCDB::CDB.new(file)
                                       end

      database.extend Persist::CDBAdapter unless Persist::CDBAdapter === database
      database.persistence_path ||= path

      database
    end

    def include?(k)
      not write? and super(k) 
    end

    def [](*args)
      write? ?  nil : super(*args)
    end

    def []=(k,v)
      if write?
        add(k,v)
      end
    end

    def closed?
      @closed
    end

    def fix_io
      if instance_variable_get(:@io) != persistence_path
        #close_read if read?
        #close_write if write?
        instance_variable_set(:@io, File.open(persistence_path))
      end
    end

    def close
      self.closed = true
      begin
        fix_io
        super
      rescue
      end
    end

    def read(force = false)
      self.closed = false
      fix_io
      open_read
    end

    def write(force = true)
      self.closed = false
      fix_io
      open_write
      self
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
              rescue
                Log.error $!.message
                Log.debug $!.backtrace * "\n"
              ensure
                read  if write?
              end
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


  def self.open_cdb(path, write, serializer = nil)
    write = true unless File.exists? path

    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::CDBAdapter.open(path, write)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
