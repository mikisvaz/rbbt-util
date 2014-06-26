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
 
  def self.paste_streams(streams, options = {})
    options = Misc.add_defaults options, :sep => "\t", :sort => true
    sort, sep, preamble = Misc.process_options options, :sort, :sep, :preamble



    out = Misc.open_pipe do |sin|

      streams = streams.collect do |stream|
        case stream
        when (defined? Step and Step) 
          stream.grace
          stream.get_stream || stream.join.path.open
        when Path
          stream.open
        when TSV::Dumper
          stream.stream
        else
          stream
        end
      end.compact

      num_streams = streams.length

      streams = streams.collect do |stream|
        sorted = Misc.sort_stream(stream)
        stream.annotate sorted if stream.respond_to? :annotate
        sorted
      end if sort

      lines         = []
      fields        = []
      sizes         = []
      key_fields    = []
      input_options = []
      empty         = []
      preambles     = []

      streams = streams.collect do |stream|
        parser = TSV::Parser.new stream, options

        lines         << parser.first_line
        empty         << stream               if parser.first_line.nil?
        key_fields    << parser.key_field
        fields        << parser.fields
        sizes         << parser.fields.length if parser.fields
        input_options << parser.options
        preambles     << parser.preamble      if TrueClass === preamble and 
                                                 not parser.preamble.empty?

        parser.stream
      end

      key_field = key_fields.compact.first
      fields = fields.compact.flatten
      options = options.merge(input_options.first)

      preamble_txt = case preamble
                     when TrueClass
                       preambles * "\n"
                     when String
                       preamble
                     else
                       nil
                     end

      header = TSV.header_lines(key_field, fields, options.merge(:preamble => preamble_txt))
      sin.puts header

      empty_pos = empty.collect{|stream| streams.index stream }
      empty_pos.sort.reverse.each do |i|
        key_fields.delete_at i
        input_options.delete_at i
      end

      begin
        done_streams = []

        keys = []
        parts = []
        lines.each_with_index do |line,i|
          if line.nil?
            keys[i] = nil
            parts[i] = nil
          else
            vs = line.chomp.split(sep, -1) 
            key, *p = vs
            keys[i] = key
            parts[i] = p
          end
          sizes[i] ||= parts[i].length-1 unless parts[i].nil?
        end

        last_min = nil
        while lines.compact.any?
          min = keys.compact.sort.first
          break if min.nil?
          str = []
          keys.each_with_index do |key,i|

            case key
            when min
              str << parts[i] * sep

              begin
                line = lines[i] = begin
                                    streams[i].gets
                                  rescue
                                    Log.exception $!
                                    nil
                                  end
                if line.nil?
                  stream = streams[i]
                  keys[i] = nil
                  parts[i] = nil
                else
                  k, *p = line.chomp.split(sep, -1)
                  raise TryAgain if k == keys[i]
                  keys[i] = k
                  parts[i] = p.collect{|e| e.nil? ? "" : e }
                end
              rescue TryAgain
                Log.warn "Skipping repeated key in stream #{i}: #{keys[i]}"
                retry
              end
            else
              if sizes[i] and sizes[i] > 0
                p = sep * (sizes[i]-1)
                str << p
              end
            end
          end

          values = str.inject(nil) do |acc,part| 
            if acc.nil?
              acc = part.dup
            else
              acc << sep << part
            end
            acc
          end
          text = [min, values] * sep
          sin.puts text
        end

        streams.each do |stream|
          stream.join if stream.respond_to? :join
        end
      rescue Aborted
        Log.error "Aborted pasting streams #{streams.inspect}: #{$!.message}"
        streams.each do |stream|
          stream.abort if stream.respond_to? :abort
        end
        raise $!
      rescue Exception
        Log.error "Exception pasting streams #{streams.inspect}: #{$!.message}"
        streams.each do |stream|
          stream.abort if stream.respond_to? :abort
        end
        raise $!
      end
    end

    out
  end

  def self.stream_flat2double(stream, options = {})
    parser = TSV::Parser.new TSV.get_stream(stream)
    dumper_options = parser.options.merge(options).merge(:type => :double)
    dumper = TSV::Dumper.new dumper_options
    dumper.init
    TSV.traverse parser, :into => dumper do |key,values|
      [key, [values]]
    end
    dumper
  end
end
