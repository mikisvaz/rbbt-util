module TSV
  def marshal_dump
    if defined?(Persist::TCAdapter) && Persist::TCAdapter === self
      super
    else
      [options, MetaExtension.purge(self)]
    end
  end
end

class Hash
  def marshal_load(array)
    options, to_hash = array
    self.merge! to_hash
    TSV.setup(self, options)
  end
end
