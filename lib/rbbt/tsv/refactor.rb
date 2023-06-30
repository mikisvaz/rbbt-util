require_relative 'stream'
module TSV
  alias original_unzip unzip
  def unzip(field = 0, merge = false, sep = ":", delete = true, **kwargs)
    kwargs[:merge] ||= merge
    kwargs[:sep] ||= sep
    kwargs[:delete] ||= delete
    original_unzip(field, **kwargs)
  end

  def swap_id(field = 0, merge = false, sep = ":", delete = true, **kwargs)
    kwargs[:merge] ||= merge
    kwargs[:sep] ||= sep
    kwargs[:delete] ||= delete
    change_id(field, **kwargs)
  end

  def swap_id(field, format, options = {}, &block)
    raise "Block support not implemented" if block_given?
    change_id(field, format, **options)
  end

  class << self
    alias original_range_index range_index
    alias original_pos_index pos_index
    def range_index(*args, filters: nil, **kwargs)
      if filters
        raise "Not implemented" if filters.length > 1
        method, value = filters.first
        method.sub!("field:", '')
        kwargs[:select] = {method => value}
      end
      original_range_index(*args, **kwargs)
    end

    def pos_index(*args, filters: nil, **kwargs)
      if filters
        raise "Not implemented" if filters.length > 1
        method, value = filters.first
        method.sub!("field:", '')
        kwargs[:select] = {method => value}
      end
      original_pos_index(*args, **kwargs)
    end

    alias original_setup setup

    def setup(*args, **kwargs, &block)
      if args.length == 2 && String === args.last
        str_setup(args.last, args.first)
      else
        original_setup(*args, **kwargs, &block)
      end
    end
  end

  def self.header_lines(key_field, fields, entry_hash = nil)
    entry_hash = entry_hash || {}
    entry_hash = entry_hash.merge(:key_field => key_field, :fields => fields)
    TSV::Dumper.header entry_hash
  end
end

Rbbt.relay_module_method TSV, :get_stream, Open, :get_stream
