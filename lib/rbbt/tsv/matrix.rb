require 'rbbt/association'

module TSV
  def self.read_matrix(tsv, field_format = "ID", value_format = "Value", *others)
    tsv = TSV.open(tsv) unless TSV === tsv
    

    if others.any?
      other_tsv = tsv.slice(others)
      tsv = tsv.slice(tsv.fields - others)
    end

    key_field, *fields = tsv.all_fields
    options = tsv.options.merge(:key_field => key_field, :fields => [field_format, value_format], :type => :double, :cast => nil)

    options[:filename] ||= tsv.filename
    options[:identifiers] ||= tsv.identifier_files.first

    dumper = TSV::Dumper.new(options)

    dumper.init
    TSV.traverse tsv, :into => dumper do |key, values|
      [key, [fields, values]]
    end

    res = TSV.open(dumper.stream, options)
    if others.any?
      other_tsv = other_tsv.to_double
      res.attach other_tsv, :one2one => true
    else
      res
    end
  end

  def matrix_melt(*args)
    tsv = TSV.read_matrix(self, *args)

    melt = Association.index tsv, :persist => false, :recycle => true
    source_field,_sep,target_field = melt.key_field.partition "~"
    melt.add_field source_field do |k,v|
      k.partition("~").first
    end
    melt.add_field target_field do |k,v|
      k.partition("~").last
    end
    melt
  end
end
