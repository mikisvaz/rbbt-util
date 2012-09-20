class FixWidthTable

  attr_accessor :filename, :file, :value_size, :record_size, :range, :size
  def initialize(filename, value_size = nil, range = nil, update = false)
    @filename = filename

    if update or %w(memory stringio).include?(filename.to_s.downcase) or not File.exists?(filename)
      Log.debug "FixWidthTable create: #{ filename }"
      @value_size  = value_size
      @range       = range
      @record_size = @value_size + (@range ? 12 : 4)

      if %w(memory stringio).include? filename.to_s.downcase
        @filename = :memory
        @file     = StringIO.new
      else
        FileUtils.rm @filename if File.exists? @filename
        @file = File.open(@filename, 'wb')
      end

      @file.write [value_size].pack("L")
      @file.write [@range ? 1 : 0 ].pack("C")
      @size = 0
    else
      Log.debug "FixWidthTable up-to-date: #{ filename }"
      @file        = File.open(@filename, 'r')
      @value_size  = @file.read(4).unpack("L").first
      @range       = @file.read(1).unpack("C").first == 1
      @record_size = @value_size + (@range ? 12 : 4)
      @size        = (File.size(@filename) - 5) / (@record_size)
    end
  end


  CONNECTIONS = {} unless defined? CONNECTIONS
  def self.get(filename, value_size = nil, range = nil, update = false)
    return self.new(filename, value_size, range, update) if filename == :memory
    case
    when (!File.exists?(filename) or update or not CONNECTIONS.include?(filename))
      CONNECTIONS[filename] = self.new(filename, value_size, range, update)
    end

    CONNECTIONS[filename] 
  end

  def format(pos, value)
    padding = value_size - value.length
    if range
      (pos  + [value + ("\0" * padding)]).pack("llla#{value_size}")
    else
      [pos, value + ("\0" * padding)].pack("la#{value_size}")
    end
  end

  def unformat(format)
    if range
      pos_start, pos_end, pos_overlap, value = format.unpack("llla#{value_size}")
      [[pos_start, pos_end, pos_overlap], value.strip]
    else
      pos, value = format.unpack("la#{value_size}")
      [pos, value.strip]
    end
  end

  def add(pos, value)
    format = format(pos, value)
    @file.write format
    @size += 1
  end
  alias << add

  def last_pos
    pos(size - 1)
  end

  def pos(index)
    return nil if index < 0 or index >= size
    @file.seek(5 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def pos_end(index)
    return nil if index < 0 or index >= size
    @file.seek(9 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def overlap(index)
    return nil if index < 0 or index >= size
    @file.seek(13 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def value(index)
    return nil if index < 0 or index >= size
    @file.seek((range ? 17 : 9 ) + (record_size) * index, IO::SEEK_SET)
    @file.read(value_size).unpack("a#{value_size}").first.strip
  end

  def read
    return if @filename == :memory
    @file.close unless @file.closed?
    @file = File.open(@filename, 'r')
  end

  def close
    @file.close
  end

  def dump
    read
    @file.rewind
    @file.read
  end

  #{{{ Adding data

  def add_point(data)
    data.sort_by{|value, pos| pos}.each do |value, pos|
      add pos, value
    end
  end

  def add_range(data)
    latest = []
    data.sort_by{|value, pos| pos[0]}.each do |value, pos|
      while latest.any? and latest[0] < pos[0]
        latest.shift
      end

      overlap = latest.length

      add pos + [overlap], value
      latest << pos[1]
    end
  end

  #{{{ Searching

  def closest(pos)
    upper = size - 1
    lower = 0

    return -1 if upper < lower

    while(upper >= lower) do
      idx = lower + (upper - lower) / 2
      comp = pos <=> pos(idx)

      if comp == 0
        break 
      elsif comp > 0
        lower = idx + 1
      else
        upper = idx - 1
      end
    end

    if pos(idx) > pos
      idx = idx - 1
    end

    idx.to_i
  end

  def get_range(pos)
    if Range === pos
      r_start = pos.begin
      r_end   = pos.end
    else
      r_start = pos.to_i
      r_end   = pos.to_i
    end

    idx = closest(r_start)

    return [] if idx >= size
    return [] if idx <0 and r_start == r_end

    idx = 0 if idx < 0

    idx -= overlap(idx) unless overlap(idx).nil?

    values = []
    l_start = pos(idx)
    l_end   = pos_end(idx)
    while l_start <= r_end
      values << value(idx) if l_end >= r_start 
      idx += 1
      break if idx >= size
      l_start = pos(idx)
      l_end   = pos_end(idx)
    end

    values
  end

  def get_point(pos)
    if Range === pos
      r_start = pos.begin
      r_end   = pos.end
    else
      r_start = pos.to_i
      r_end   = pos.to_i
    end

    idx = closest(r_start)

    return [] if idx >= size
    return [] if idx <0 and r_start == r_end

    idx = 0 if idx < 0

    idx += 1 unless pos(idx) >= r_start

    return [] if idx >= size

    values = []
    l_start = pos(idx)
    l_end   = pos_end(idx)
    while l_start <= r_end
      values << value(idx)
      idx += 1
      break if idx >= size
      l_start = pos(idx)
      l_end   = pos_end(idx)
    end

    values
  end

  def [](pos)
    return [] if size == 0
    if range
      get_range(pos)
    else
      get_point(pos)
    end
  end

  def values_at(*list)
    list.collect{|pos|
      self[pos]
    }
  end

end
