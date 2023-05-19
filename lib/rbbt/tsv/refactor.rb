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

end
