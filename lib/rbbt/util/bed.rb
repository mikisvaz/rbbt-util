require 'rbbt/util/tsv'
require 'rbbt/util/misc'
require 'rbbt/util/log'
require 'yaml'

class Bed

  class Entry < Struct.new( :value, :start, :end, :overlap); end

  class FixWidthTable
    SERIALIZER = Marshal
    def self.serialize(entry)
      SERIALIZER.dump(entry)
    end

    def self.deserialise(entry)
      SERIALIZER.parse(entry)
    end

    def self.format(entry, record_size)
      data = serialize(entry)
      padding = record_size - data.length
      [data + "\0" * padding].pack("a#{record_size}")
    end

    def self.unformat(format, record_size)
      data = format.unpack("a#{record_size}").first
      SERIALIZER.load(data)
    end

    def self.get_record_size(entries)
      max = 0
      entries.each do |entry|
        size = serialize(entry).length
        max  = size if size > max
      end

      max
    end

    attr_accessor :size
    def initialize(file, record_size = nil, rewrite = false)
      @filename = file

      if rewrite or not File.exists? file
        Log.debug("Opening FixWidthTable in #{ file } writing. Record size: #{record_size}")
        @file = File.open(@filename, 'wb')
        @record_size = record_size
        @file.write [record_size].pack("S")
        @file.seek 2, IO::SEEK_SET
        @size = 2
      else
        Log.debug("Opening FixWidthTable in #{ file } for reading")
        @file = File.open(@filename, 'rb')
        @record_size = @file.read(2).unpack("S").first
        @size = (File.size(@filename) - 2) / @record_size
        Log.debug("Record size #{@record_size}")
      end
    end

    def read
      @file.close 
      @file = File.open(@filename, 'rb')
    end

    def add(entry)
      @size += @record_size
      format = FixWidthTable.format(entry, @record_size)
      @file.write format
    end

    def [](index)
      Log.debug("Getting Index #{ index }")
      return nil if index < 0 or index >= size
      @file.seek(2 + (@record_size) * index, IO::SEEK_SET)

      format = @file.read(@record_size)
      FixWidthTable.unformat(format, @record_size)
    end
  end

  #{{{ Persistence

  CACHEDIR="/tmp/bed_persistent_cache"
  FileUtils.mkdir CACHEDIR unless File.exist? CACHEDIR

  def self.cachedir=(cachedir)
    CACHEDIR.replace cachedir
    FileUtils.mkdir_p CACHEDIR unless File.exist? CACHEDIR
  end

  def self.cachedir
    CACHEDIR
  end

  def self.get_persistence_file(file, prefix, options = {})
    File.join(CACHEDIR, prefix.gsub(/\s/,'_').gsub(/\//,'>') + Digest::MD5.hexdigest([file, options].inspect))
  end


  attr_accessor :index, :range
  def initialize(tsv, options = {})
    options = Misc.add_defaults options, :range => nil, :key => 0, :value => 1, :persistence => false, :persistence_file => nil, :tsv => {}
  
    filename = nil
    case
    when TSV === tsv
      filename = tsv.filename
    when (String === tsv and File.exists? tsv.sub(/#.*/,''))
      filename = tsv
    else 
      filename = "None"
    end


    if options[:range]
      options[:key]   = options[:range].first
      options[:value] = [options[:value], options[:range].last]
      @range = true
    else
      @range = false
    end

    if options[:persistence] and options[:persistence_file].nil?
      options[:persistence_file] = Bed.get_persistence_file(filename, (options[:range].nil? ? "Point:" : "Range:"),  options)
    end

    if options[:persistence_file] and File.exists? options[:persistence_file]
      @index = FixWidthTable.new options[:persistence_file]
      return
    end

    tsv = TSV.new(tsv, options[:tsv]) unless TSV === tsv

    @index = []
    max_size = 0
    entry = nil
    tsv.through options[:key], options[:value] do |key, values|
      if @range
        entry = Entry.new(values[0], key.to_i, values[1].to_i,  nil)
      else
        entry = Entry.new(values[0], key.to_i, nil, nil)
      end
      max_size =
        @index << entry
    end

    @index.sort!{|a,b| a.start <=> b.start}

    if range
      latest = []
      @index.each do |entry|
        while latest.any? and latest[0] < entry.start
          latest.shift
        end

        entry.overlap = latest.length
        latest << entry.end
      end
    end

    if options[:persistence_file]
      record_size = FixWidthTable.get_record_size(@index)

      table = FixWidthTable.new options[:persistence_file], record_size
      @index.each do |entry| table.add entry end
      table.read

      @index = table
    end
  end

  def closest(pos)
    upper = @index.size - 1
    lower = 0

    return -1 if upper < lower

    while(upper >= lower) do
      idx = lower + (upper - lower) / 2
      comp = pos <=> @index[idx].start

      if comp == 0
        break 
      elsif comp > 0
        lower = idx + 1
      else
        upper = idx - 1
      end
    end

    if @index[idx].start > pos
      idx = idx - 1
    end

    idx
  end

  def get_range(pos)
    if Range === pos
      r_start = pos.begin
      r_end   = pos.end
    else
      r_start = pos
      r_end   = pos
    end

    idx = closest(r_start)
    
    return [] if idx < 0 

    idx -= @index[idx].overlap if @index[idx].overlap

    values = []
    l = @index[idx]
    while l.start <= r_end
      values << l.value if l.end >= r_start 
      idx += 1
      l = @index[idx]
      break if l.nil?
    end

    values
  end

  def get_point(pos)
    if Range === pos
      r_start = pos.begin
      r_end   = pos.end
    else
      r_start = pos
      r_end   = pos
    end


    idx = closest(r_start) 
    idx += 1 unless @index[idx].start == r_start

    values = []
    l = @index[idx]
    while l.start <= r_end
      values << l.value
      idx += 1
      l = @index[idx]
    end

    values
  end

  def [](pos)
    if range
      get_range(pos)
    else
      get_point(pos)
    end
  end

end
