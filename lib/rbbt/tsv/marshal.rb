module TSV
  def marshal_dump
    if defined?(Persist::TCAdapter) && Persist::TCAdapter === self
      super
    else
      [info, to_hash]
    end
  end
end

class Hash
  def marshal_load(array)
    info, to_hash = array
    self.merge! to_hash
    TSV.setup(self)
  end
end
