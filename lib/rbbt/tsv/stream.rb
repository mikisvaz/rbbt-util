module TSV

  #def self.collapse_stream(input, options = {}, &block)
  #  options = IndiferentHash.add_defaults options, :sep => "\t", :header_hash => '#', :uniq => true
  #  input_stream = TSV.get_stream input

  #  header_hash = options[:header_hash]
  #  cmd_args = options[:uniq] ? "-u" : nil

  #  sorted_input_stream = Open.sort_stream input_stream, header_hash, cmd_args

  #  parser = TSV::Parser.new(sorted_input_stream, options.dup)
  #  dumper = TSV::Dumper.new parser
  #  header = TSV.header_lines(parser.key_field, parser.fields, parser.options)
  #  dumper.close_in
  #  dumper.close_out
  #  dumper.stream = Open.collapse_stream parser.stream, parser.first_line, parser.sep, header, &block
  #  dumper
  #end
 
#  def self.paste_streams(streams, options = {})
#    options = IndiferentHash.add_defaults options, :sep => "\t", :sort => true
#    sort, sep, preamble, header, same_fields, fix_flat, all_match, field_prefix = IndiferentHash.process_options options, :sort, :sep, :preamble, :header, :same_fields, :fix_flat, :all_match, :field_prefix
#
#    out = Open.open_pipe do |sin|
#
#      streams = streams.collect do |stream|
#        case stream
#        when (defined? Step and Step) 
#          stream.grace
#          stream.stream || Open.open(stream.join.path)
#        when Path
#          stream.open
#        when TSV::Dumper
#          stream.stream
#        else
#          stream
#        end
#      end.compact
#
#      num_streams = streams.length
#
#      streams = streams.collect do |stream|
#        sorted = Open.sort_stream(stream)
#        stream.annotate sorted if stream.respond_to? :annotate
#        sorted
#      end if sort
#
#      lines         = []
#      fields        = []
#      sizes         = []
#      key_fields    = []
#      input_options = []
#      empty         = []
#      preambles     = []
#
#      streams = streams.collect do |stream|
#
#        parser = TSV::Parser.new stream, **options.dup
#        sfields = parser.fields
#
#        if field_prefix
#          index = streams.index stream
#          prefix = field_prefix[index]
#
#          sfields = sfields.collect{|f| [prefix, f] * ":" }
#        end
#
#        first_line = parser.first_line
#        first_line = nil if first_line == ""
#
#        lines         << first_line
#        key_fields    << parser.key_field
#        fields        << sfields
#        sizes         << sfields.length if sfields
#        input_options << parser.options
#        preambles     << parser.preamble      if preamble and not parser.preamble.empty?
#
#        stream = if fix_flat and parser.type == :flat and first_line
#                   parts = lines[-1].nil? ? [] : lines[-1].split("\t")
#                   lines[-1] = [parts[0], (parts[1..-1] || [])*"|"] * "\t"
#                   TSV.stream_flat2double(parser.stream, :noheader => true).stream
#                 else
#                   parser.stream
#                 end
#
#        empty         << stream               if parser.first_line.nil? || parser.first_line.empty?
#
#        stream
#      end
#
#      all_fields = fields
#      key_field = key_fields.compact.first
#      if same_fields
#        fields = fields.first
#      else
#        fields = fields.compact.flatten
#      end
#      options = options.merge(input_options.first || {})
#      options[:type] = :list if options[:type] == :single
#      options[:type] = :double if fix_flat
#
#      preamble_txt = case preamble
#                     when TrueClass
#                       preambles * "\n"
#                     when String
#                       if preamble[0] == '+'
#                         preambles * "\n" + "\n" + preamble[1..-1]
#                       else
#                         preamble
#                       end
#                     else
#                       nil
#                     end
#
#      header ||= TSV.header_lines(key_field, fields, options.merge(:preamble => preamble_txt))
#      sin.puts header
#
#      empty_pos = empty.collect{|stream| streams.index stream }
#      empty_pos.sort.reverse.each do |i|
#        key_fields.delete_at i
#        input_options.delete_at i
#      end
#
#      begin
#        done_streams = []
#
#        keys = []
#        parts = []
#        lines.each_with_index do |line,i|
#          if line.nil? || line.empty?
#            keys[i] = nil
#            parts[i] = nil
#          else
#            vs = line.chomp.split(sep, -1) 
#            key, *p = vs
#            keys[i] = key
#            parts[i] = p
#          end
#          sizes[i] ||= parts[i].length-1 unless parts[i].nil?
#        end
#
#        last_min = nil
#        while lines.compact.any?
#          min = keys.compact.sort.first
#          break if min.nil?
#          str = []
#
#          skip = all_match && keys.uniq != [min]
#
#          keys.each_with_index do |key,i|
#            case key
#            when min
#              str << parts[i] * sep
#
#              begin
#                line = lines[i] = begin
#                                    streams[i].gets
#                                  rescue
#                                    Log.exception $!
#                                    nil
#                                  end
#                if line.nil?
#                  stream = streams[i]
#                  keys[i] = nil
#                  parts[i] = nil
#                else
#                  k, *p = line.chomp.split(sep, -1)
#                  raise TryAgain if k == keys[i]
#                  keys[i] = k
#                  parts[i] = p.collect{|e| e.nil? ? "" : e }
#                end
#              rescue TryAgain
#                Log.debug "Skipping repeated key in stream #{i}: #{keys[i]}"
#                retry
#              end
#            else
#              if sizes[i] and sizes[i] > 0
#                p = sep * (sizes[i]-1)
#                str << p
#              end
#            end
#          end
#
#          next if skip
#
#          if same_fields
#
#            values = nil
#            str.each do |part|
#              next if part.nil? or part.empty?
#              _p = part.split(sep,-1)
#              if values.nil?
#                values = _p.collect{|v| [v]}
#              else
#                _p.each_with_index{|v,i| values[i] ||= []; values[i] << v}
#              end
#            end
#
#            values = [[]] * str.length if values.nil?
#            values = values.collect{|list| list * "|" } * sep
#
#          else
#            values = str.inject(nil) do |acc,part| 
#              if acc.nil?
#                acc = part.dup
#              else
#                acc << sep << part
#              end
#              acc
#            end
#          end
#          text = [min, values] * sep
#          sin.puts text
#        end
#
#        streams.each do |stream|
#          stream.join if stream.respond_to? :join
#        end
#      rescue Aborted
#        Log.error "Aborted pasting streams #{streams.inspect}: #{$!.message}"
#        streams.each do |stream|
#          stream.abort if stream.respond_to? :abort
#        end
#        raise $!
#      rescue Exception
#        Log.error "Exception pasting streams #{streams.inspect}: #{$!.message}"
#        streams.each do |stream|
#          stream.abort if stream.respond_to? :abort
#        end
#        raise $!
#      end
#    end
#
#    out
#  end

#  def self.stream_flat2double(stream, options = {})
#    noheader = IndiferentHash.process_options options, :noheader
#    parser = TSV::Parser.new TSV.get_stream(stream), :type => :flat
#    dumper_options = parser.options.merge(options).merge(:type => :double)
#    dumper = TSV::Dumper.new dumper_options
#    dumper.init unless noheader
#    TSV.traverse parser, :into => dumper do |key,values|
#      key = key.first if Array === key
#      values = [values] unless Array === values
#      [key, [values.flatten]]
#    end
#    dumper
#  end


  def self.reorder_stream(stream, positions, sep = "\t")
    Open.open_pipe do |sin|
      line = stream.gets
      line.chomp! unless line.nil?

      while line =~ /^#\:/
        sin.puts line
        line = stream.gets
        line.chomp! unless line.nil?
      end

      while line  =~ /^#/
        if Hash === positions
          new = (0..line.split(sep,-1).length-1).to_a
          positions.each do |k,v|
            new[k] = v
            new[v] = k
          end
          positions = new
        end
        sin.puts "#" + line.sub(/^#/,'').chomp.split(sep).values_at(*positions).compact * sep
        line = stream.gets
        line.chomp! unless line.nil?
      end

      while line
        if Hash === positions
          new = (0..line.split(sep, -1).length-1).to_a
          positions.each do |k,v|
            new[k] = v
            new[v] = k
          end
          positions = new
        end
        values = line.split(sep, -1)
        new_values = values.values_at(*positions)
        sin.puts new_values * sep
        line = stream.gets
        line.chomp! unless line.nil?
      end
    end
  end


  def self.reorder_stream_tsv(stream, key_field, fields=nil, zipped = true, bar = nil)
    parser = TSV::Parser.new TSV.get_stream(stream), :key_field => key_field, :fields => fields
    dumper_options = parser.options
    dumper = TSV::Dumper.new dumper_options
    dumper.init 
    case parser.type
    when :single
      TSV.traverse parser, :into => dumper, :bar => bar do |keys,values|
        key = keys.first
        [key, [values]]
      end
    when :double
      TSV.traverse parser, :into => dumper, :bar => bar do |keys,values|
        res = []
        keys.each_with_index do |key,i|
          vs = zipped ?  values.collect{|l| l.length == 1 ? l : [l[i]] } : values
          res << [key, vs]
        end
        res.extend MultipleResult
        res
      end
    when :list
      TSV.traverse parser, :into => dumper, :bar => bar do |keys,values|
        key = keys === Array ? keys.first : keys
        [key, values]
      end
    when :flat
      TSV.traverse parser, :into => dumper, :bar => bar do |keys,values|
        key = keys === Array ? keys.first : keys
        [key, values]
      end
    else
      raise "Unknown type: " << parser.type.to_s
    end
    dumper
  end

end
