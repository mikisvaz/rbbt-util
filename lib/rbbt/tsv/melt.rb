module TSV
  def self.melt(tsv, key_field, header_field, fields, *info_fields, &block)
    info_fields.unshift header_field
    TSV.traverse tsv, :into => :dumper, :key_field => key_field, :fields => info_fields do |k,values|
      res = fields.zip(values).collect do |field, value|
        info_values = if block_given?
                        new = block.call value
                        next if new.nil?
                        new
                      else
                        [value]
                      end
        info_values.unshift field
        [field, info_values]
      end
      res.extend MultipleResult
      res
    end
  end

  def melt(header_field, *info_fields, &block)
    TSV.melt self, key_field, header_field, fields, *info_fields, &block
  end
end
