require 'rbbt/fix_width_table'

module Persist

  module FWTAdapter
    include Persist::TSVAdapter

    attr_accessor :pos_function

    def self.open(path, value_size, range = false, update = false, in_memory = false, &pos_function)
      db = CONNECTIONS[path] ||= FixWidthTable.new(path, value_size, range, update, in_memory)
      db.extend Persist::FWTAdapter
      db.persistence_path = path
      db.pos_function = pos_function
      db
    end

    def persistence_path=(value)
      @persistence_path = value
      @filename = value
    end

    def metadata_file
      @metadata_file ||= self.persistence_path + '.metadata'
    end

    def metadata
      return {} unless File.exists? metadata_file
      Open.open(metadata_file, :mode => "rb") do |f|
        Marshal.load(f)
      end
    end

    def set_metadata(k,v)
      metadata = self.metadata
      metadata[k] = v
      Misc.sensiblewrite(metadata_file, Marshal.dump(metadata))
    end

    def [](key)
      if TSV::ENTRY_KEYS.include? key
        metadata[key]
      else
        key = pos_function.call(key) if pos_function
        res = super(key)
        res.extend MultipleResult
        res
      end
    end

    def []=(key, value)
      if TSV::ENTRY_KEYS.include? key
        set_metadata(key, value)
      else
        if range
          add_range_point key, value
        else
          add key, value
        end
      end
    end
     
    def add(key, value)
      key = pos_function.call(key) if pos_function and not (range and Array === key)
      super(key, value)
    end

    def add_range_point(key, value)
      key = pos_function.call(key) if pos_function
      super(key, value)
    end

    def <<(key, value)
      self.send(:[]=, *i)
    end

    def include?(i)
      return true if Fixnum === i and i < pos(@size)
      return true if metadata.include? i
      false
    end

    def size
      @size #+ metadata.keys.length
    end

    def each
      @size.times do |i|
        yield i, value(i)
      end
    end

    def keys
      []
    end
  end

  def self.open_fwt(path, value_size, range = false, serializer = nil, update = false, in_memory = false, &pos_function)
    FileUtils.mkdir_p File.dirname(path) unless File.exists?(File.dirname(path))

    database = Persist::FWTAdapter.open(path, value_size, range, update, in_memory, &pos_function)

    unless serializer == :clean
      TSV.setup database
      database.serializer = serializer || database.serializer
    end

    database
  end
end
