require 'rbbt/tsv/parser'
require 'rbbt/tsv/dumper'
module TSV

  def self.collapse_stream(input, options = {})
    options = Misc.add_defaults options, :sep => "\t"
    input_stream = TSV.get_stream input

    sorted_input_stream = Misc.sort_stream input_stream

    parser = TSV::Parser.new sorted_input_stream, options.dup
    dumper = TSV::Dumper.new parser
    header = TSV.header_lines(parser.key_field, parser.fields, parser.options)
    dumper.close_in
    dumper.close_out
    dumper.stream = Misc.collapse_stream parser.stream, parser.first_line, parser.sep, header
    dumper
  end
 
  def self.paste_streams(inputs, options = {})
    options = Misc.add_defaults options, :sep => "\t", :sort => false
    sort = Misc.process_options options, :sort

    input_streams = []
    input_lines = []
    input_fields = []
    input_key_fields = []
    input_options = []

    inputs.each do |input|
      stream = TSV.get_stream input
      stream = sort ? Misc.sort_stream(stream) : stream
      parser = TSV::Parser.new stream, options
      input_streams << parser.stream
      input_lines << parser.first_line
      input_fields << parser.fields
      input_key_fields << parser.key_field
      input_options << parser.options
    end

    key_field = input_key_fields.first
    fields = input_fields.flatten
    options = options.merge(input_options.first)

    dumper = TSV::Dumper.new options.merge(:key_field => key_field, :fields => fields)
    dumper.close_in
    dumper.close_out
    header = TSV.header_lines(key_field, fields, options)
    dumper.stream = Misc.paste_streams input_streams, input_lines, options[:sep], header
    dumper
  end
end
