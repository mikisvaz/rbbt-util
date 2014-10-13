require 'rbbt/persist'
require 'set'

module Bgzf
  attr_accessor :data_offset, :compressed_stream, :block_cache_size

  def self.setup(compressed_stream)
    require 'bio-bgzf'
    reader = Bio::BGZF::Reader.new(compressed_stream)
    reader.extend Bgzf
    reader.compressed_stream = compressed_stream
    reader.data_offset = 0
    reader
  end

  def filename
    @filename ||= begin
                  compressed_stream.respond_to?(:filename) ? compressed_stream.filename : nil
                end
  end

  def closed?
    @compressed_stream.closed?
  end

  def close
    @compressed_stream.close unless @compressed_stream.closed?
    @access.clear if @access
    @blocks.clear if @blocks
  end

  def seek(off)
    @data_offset = off
  end

  def _index
    @_index ||= begin
                  index = Persist.persist("BGZF index" + (filename || "").sub(/.bgz$/,''), :marshal, :dir => Rbbt.var.bgzf_index) do
                    index = []
                    pos = 0
                    while true do
                      blockdata_offset = tell
                      block = begin
                                read_block
                              rescue Exception
                                raise "BGZF seems to be buggy so some compressed files will not decompress right. Try uncompressing #{filename}" if $!.message =~ /BGFZ.*expected/
                                raise $!
                              end
                      break unless block
                      index << [pos, blockdata_offset]
                      pos += block.length
                    end
                    index
                  end
                  @block_cache_size = Math.log(index.length).to_i + 1
                  index
               end
  end

  def read_all
    str = ""
    while true
      block = read_block
      break if block.nil?
      str << block
    end
    str
  end

  def init
    _index
  end

  def closest_page(pos)
    upper = _index.size - 1
    lower = 0
    @_index_pos ||= _index.collect{|v| v.first }

    return -1 if upper < lower

    while(upper >= lower) do
      idx = (idx.nil? and @last_idx) ? @last_idx : (lower + (upper - lower) / 2)
      pos_idx = @_index_pos[idx]

      case pos <=> pos_idx
      when 0
        break
      when -1
        upper = idx - 1
      when 1
        lower = idx + 1
      end
    end

    @last_idx = idx

    if pos_idx > pos
      idx = idx - 1
    end


    idx.to_i
  end

  def block_offset
    pos = data_offset
    i = closest_page(data_offset)
    page = _index[i][1]
    offset = pos - _index[i][0]
    [page, offset]
  end

  def _purge_cache
    if @blocks.length > @block_cache_size
      @access.uniq!
      oldest = @access.last
      @blocks.delete oldest
    end
  end

  def _get_block(vo)
    @blocks ||= {}
    @access ||= []
    @access << vo
    if @blocks.include? vo
      @blocks[vo]
    else
      _purge_cache
      @blocks[vo] ||= read_block_at vo
    end
  end

  def get_block
    block_vo, offset = block_offset
    block = _get_block block_vo
    block[offset..-1]
  end

  def read(size=nil)
    return read_all if size.nil?

    block = get_block 
    return "" if block.nil? or block.empty?
    len = block.length
    if len >= size
      @data_offset += size
      return block[0..size-1]
    else
      @data_offset += len
      str = block
      str << read(size - len)
      str
    end
  end

  def getc
    read(1)
  end

  def gets
    str = nil
    current = @data_offset
    while true
      block = read(1024)
      break if block.empty?
      str = "" if str.nil?
      if i = block.index("\n")
        str << block[0..i]
        break
      else
        str << block
      end
    end
    return nil if str.nil?

    @data_offset = current + str.length

    str
  end
end
