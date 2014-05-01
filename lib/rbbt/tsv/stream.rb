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

    input_source_streams = inputs.collect do |input|
      stream = sort ? Misc.sort_stream(input) : TSV.get_stream(input)
      stream
    end

    input_source_streams.each do |stream|
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

  def self.paste_streams(streams, options = {})
    options = Misc.add_defaults options, :sep => "\t", :sort => true
    sort, sep = Misc.process_options options, :sort, :sep

    Misc.open_pipe do |sin|
      num_streams = streams.length

      streams = streams.collect do |stream|
        if defined? Step and Step === stream
          stream.get_stream || stream.join.path.open
        else
          stream
        end
      end

      streams = streams.collect do |stream|
        Misc.sort_stream(stream)
      end if sort

      lines = []
      fields = []
      key_fields = []
      input_options = []
      empty = []

      streams = streams.collect do |stream|
        parser = TSV::Parser.new stream, options
        lines << parser.first_line
        empty << stream if parser.first_line.nil?
        key_fields << parser.key_field
        fields << parser.fields
        input_options << parser.options

        parser.stream
      end

      key_field = key_fields.compact.first
      fields = fields.compact.flatten
      options = options.merge(input_options.first)

      sin.puts TSV.header_lines(key_field, fields, options)

      empty.each do |stream|
        i = streams.index stream
        lines.delete_at i
        fields.delete_at i
        key_fields.delete_at i
        input_options.delete_at i
      end

      begin
        done_streams = []

        keys = []
        parts = []
        lines.each_with_index do |line,i|
          key, *p = line.strip.split(sep, -1) 
          keys[i] = key
          parts[i] = p
        end
        sizes = parts.collect{|p| p.length }
        last_min = nil
        while lines.compact.any?
          min = keys.compact.sort.first
          str = []
          keys.each_with_index do |key,i|
            case key
            when min
              str << [parts[i] * sep]
              line = lines[i] = begin
                                  streams[i].gets
                                rescue
                                  Log.exception $!
                                  nil
                                end
              if line.nil?
                stream = streams[i]
                stream.join if stream.respond_to? :join
                keys[i] = nil
                parts[i] = nil
              else
                k, *p = line.strip.split(sep, -1)
                keys[i] = k
                parts[i] = p
              end
            else
              str << [sep * (sizes[i]-1)] if sizes[i] > 0
            end
          end

          sin.puts [min, str*sep] * sep
        end
        streams.each do |stream|
          stream.join if stream.respond_to? :join
        end
      rescue Exception
        ts = streams.collect do |stream|
          Thread.new do
            stream.abort if stream.respond_to? :abort
          end
        end
        ts.each do |t| t.join end
        raise $!
      end
    end
  end
end
