module TSV
  def self.read_matrix(tsv, field_format = "ID", value_format = "Value")
    tsv = TSV.open(tsv) unless TSV === tsv

    key_field, *fields = tsv.all_fields
    options = tsv.options.merge(:key_field => key_field, :fields => [field_format, value_format], :type => :double, :cast => nil)

    options[:filename] ||= tsv.filename
    options[:identifiers] ||= tsv.identifier_files.first

    dumper = TSV::Dumper.new(options)

    dumper.init
    TSV.traverse tsv, :into => dumper do |key, values|
      [key, [fields, values]]
    end

    dumper.stream
  end
end
