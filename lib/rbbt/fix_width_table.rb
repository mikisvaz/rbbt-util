class FixWidthTable

  attr_accessor :filename, :file, :value_size, :record_size, :range, :size, :mask, :write
  def initialize(filename, value_size = nil, range = nil, update = false, in_memory = true)
    @filename = filename

    if update || %w(memory stringio).include?(filename.to_s.downcase) || ! File.exist?(filename)
      Log.debug "FixWidthTable create: #{ filename }"
      @value_size  = value_size
      @range       = range
      @record_size = @value_size + (@range ? 16 : 8)
      @write = true

      if %w(memory stringio).include?(filename.to_s.downcase)
        @filename = :memory
        @file     = StringIO.new
      else
        FileUtils.rm @filename if File.exist? @filename
        FileUtils.mkdir_p File.dirname(@filename) unless File.exist? @filename
        #@file = File.open(@filename, 'wb')
        @file = File.open(@filename, 'w:ASCII-8BIT')
      end

      @file.write [value_size].pack("L")
      @file.write [@range ? 1 : 0 ].pack("C")

      @size = 0
    else
      Log.debug "FixWidthTable up-to-date: #{ filename } - (in_memory:#{in_memory})"
      if in_memory
        @file        = Open.open(@filename, :mode => 'r:ASCII-8BIT'){|f| StringIO.new f.read}
      else
        @file        = File.open(@filename, 'r:ASCII-8BIT')
      end
      @value_size  = @file.read(4).unpack("L").first
      @range       = @file.read(1).unpack("C").first == 1
      @record_size = @value_size + (@range ? 16 : 8)
      @write = false

      @size        = (File.size(@filename) - 5) / (@record_size)
    end

    @mask = "a#{@value_size}"
  end

  def write?
    @write
  end

  def persistence_path
    @filename
  end

  def persistence_path=(value)
    @filename=value
  end

  def self.get(filename, value_size = nil, range = nil, update = false)
    return self.new(filename, value_size, range, update) if filename == :memory
    case
    when (!File.exist?(filename) or update or not Persist::CONNECTIONS.include?(filename))
      Persist::CONNECTIONS[filename] = self.new(filename, value_size, range, update)
    end

    Persist::CONNECTIONS[filename] 
  end

  def format(pos, value)
    padding = value_size - value.length
    if range
      (pos  + [padding, value + ("\0" * padding)]).pack("llll#{mask}")
    else
      [pos, padding, value + ("\0" * padding)].pack("ll#{mask}")
    end
  end

  def add(pos, value)
    format = format(pos, value)
    @file.write format

    @size += 1
  end

  def last_pos
    pos(size - 1)
  end

  def idx_pos(index)
    return nil if index < 0 or index >= size
    @file.seek(5 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def idx_pos_end(index)
    return nil if index < 0 or index >= size
    @file.seek(9 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def idx_overlap(index)
    return nil if index < 0 or index >= size
    @file.seek(13 + (record_size) * index, IO::SEEK_SET)
    @file.read(4).unpack("l").first
  end

  def idx_value(index)
    return nil if index < 0 or index >= size
    @file.seek((range ? 17 : 9 ) + (record_size) * index, IO::SEEK_SET)
    padding = @file.read(4).unpack("l").first+1
    txt = @file.read(value_size)
    str = txt.unpack(mask).first
    padding > 1 ? str[0..-padding] : str
  end

  def read(force = false)
    return if @filename == :memory
    @write = false
    @file.close unless @file.closed?
    @file = File.open(filename, 'r:ASCII-8BIT')
  end

  def close
    @write = false
    @file.close
  end

  def dump
    read
    @file.rewind
    @file.read
  end

  #{{{ Adding data

  def add_point(data)
    data.sort_by{|value, pos| pos }.each do |value, pos|
      add pos, value
    end
  end

  def add_range_point(pos, value)
    @latest ||= []
    while @latest.any? and @latest[0] < pos[0]
      @latest.shift
    end
    overlap = @latest.length
    add pos + [overlap], value
    @latest << pos[1]
  end

  def add_range(data)
    @latest = []
    data.sort_by{|value, pos| pos[0] }.each do |value, pos|
      add_range_point(pos, value)
    end
  end

  #{{{ Searching

  def closest(pos)
    upper = size - 1
    lower = 0

    return -1 if upper < lower

    while(upper >= lower) do
      idx = lower + (upper - lower) / 2
      pos_idx = idx_pos(idx)

      case pos <=> pos_idx
      when 0
        break
      when -1
        upper = idx - 1
      when 1
        lower = idx + 1
      end
    end

    if pos_idx > pos
      idx = idx - 1
    end

    idx.to_i
  end

  def get_range(pos, return_idx = false)
    case pos
    when Range
      r_start = pos.begin
      r_end   = pos.end
    when Array
      r_start, r_end = pos
    else
      r_start, r_end = pos, pos
    end

    idx = closest(r_start)

    return [] if idx >= size
    return [] if idx < 0 and r_start == r_end

    idx = 0 if idx < 0

    overlap = idx_overlap(idx)

    idx -= overlap unless overlap.nil?

    values = []
    l_start = idx_pos(idx)
    l_end   = idx_pos_end(idx)
    
    if return_idx
      while l_start <= r_end
        values << idx if l_end >= r_start 
        idx += 1
        break if idx >= size
        l_start = idx_pos(idx)
        l_end   = idx_pos_end(idx)
      end
    else
      while l_start <= r_end
        values << idx_value(idx) if l_end >= r_start 
        idx += 1
        break if idx >= size
        l_start = idx_pos(idx)
        l_end   = idx_pos_end(idx)
      end
    end

    values
  end

  def get_point(pos, return_idx = false)
    if Range === pos
      r_start = pos.begin
      r_end   = pos.end
    else
      r_start = pos.to_i
      r_end   = pos.to_i
    end

    idx = closest(r_start)

    return [] if idx >= size
    return [] if idx < 0 and r_start == r_end

    idx = 0 if idx < 0

    idx += 1 unless idx_pos(idx) >= r_start

    return [] if idx >= size

    values = []
    l_start = idx_pos(idx)
    l_end   = idx_pos_end(idx)
    if return_idx 
      while l_start <= r_end
        values << idx
        idx += 1
        break if idx >= size
        l_start = idx_pos(idx)
        l_end   = idx_pos_end(idx)
      end
    else
      while l_start <= r_end
        values << idx_value(idx)
        idx += 1
        break if idx >= size
        l_start = idx_pos(idx)
        l_end   = idx_pos_end(idx)
      end
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
  
  def overlaps(pos, value = false)
    return [] if size == 0
    idxs = if range
      get_range(pos, true)
    else
      get_point(pos, true)
    end
    if value
      idxs.collect{|idx| [idx_pos(idx), idx_pos_end(idx), idx_value(idx)] * ":"}
    else
      idxs.collect{|idx| [idx_pos(idx), idx_pos_end(idx)] * ":"}
    end
  end


  def values_at(*list)
    list.collect{|pos|
      self[pos]
    }
  end

  def chunked_values_at(keys, max = 5000)
    Misc.ordered_divide(keys, max).inject([]) do |acc,c|
      new = self.values_at(*c)
      new.annotate acc if new.respond_to? :annotate and acc.empty?
      acc.concat(new)
    end
  end
end
