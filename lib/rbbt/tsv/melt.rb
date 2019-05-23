module TSV
  def self.melt(tsv, key_field, header_field, fields, *info_fields, &block)
    dumper = TSV::Dumper.new :key_field => "ID", :fields => [key_field] + info_fields, :type => :list
    dumper.init
    TSV.traverse tsv, :into => dumper, :fields => info_fields do |k,values|
      values = [values] if tsv.type == :single
      values = values.collect{|v| [v]} if tsv.type == :list
      values = Misc.zip_fields(values) if tsv.type == :double

      res = []
      values.each_with_index do |value,i|
        info_values = if block_given?
                        new = block.call value
                        next if new.nil?
                        new
                      else
                        value
                      end
        
        info_values = [info_values] unless tsv.type == :double
        id = [k, i] * ":"
        res << [id, [k] + [info_values].flatten]
      end
      res.extend MultipleResult
      res
    end
  end

  def melt(header_field = nil, *info_fields, &block)
    info_fields = fields if info_fields.nil? || info_fields.empty?
    TSV.melt self, key_field, header_field, fields, *info_fields, &block
  end
end
