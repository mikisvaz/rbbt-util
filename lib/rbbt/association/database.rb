require 'rbbt/association/util'
require 'rbbt/tsv/change_id'

module Association

  def self.add_reciprocal(tsv)
    new = TSV.open(tsv.dumper_stream)
    tsv.with_unnamed do
      tsv.through do |source, values|
        next if values.flatten.compact.empty?
        if values.length > 1
          Misc.zip_fields(values).each do |_target_values|
            target, *target_values = _target_values
            if new[target].nil?
              new_values = [[source]] + target_values.collect{|v| [v] }
              new[target] = new_values
            else
              new_values = new[target].collect{|l| l.dup }
              targets = new_values.shift
              targets << source
              rest = new_values.zip(target_values).collect do |o,n|
                o << n
                o
              end
              new_values = [targets] + rest
              new[target] = new_values
            end
          end
        else
          values.first.each do |target|
            if new[target].nil?
              new[target] = [[source]]
            else
              new[target] = [new[target][0] + [source]]
            end
          end
        end
      end
    end

    tsv.annotate(new)

    new
  end

  def self.add_reciprocal(tsv)
    new = TSV.open(tsv.dumper_stream)
    tsv.with_unnamed do
      case tsv.type
      when :double
        tsv.through do |source, values|
          Misc.zip_fields(values).each do |info|
            target, *rest = info
            new.zip_new target, [source] + rest
          end
        end
      else
      end
    end

    tsv.annotate(new)

    new
  end

  def self.translate(tsv, source_final_format, target_final_format, options = {})
    source_field = tsv.key_field
    target_field = tsv.fields.first

    if source_final_format and source_field != source_final_format 
      Log.debug("Changing source format from #{tsv.key_field} to #{source_final_format}")

      tsv = TSV.translate(tsv, source_field, source_final_format, options)
    end

    # Translate target 
    if target_final_format and target_field != target_final_format
      Log.debug("Changing target format from #{target_field} to #{target_final_format}")
      old_key_field = tsv.key_field 
      tsv.key_field = "MASK"
      tsv = TSV.translate(tsv, target_field, target_final_format, options)
      tsv.key_field = old_key_field
    end

    tsv
  end

  def self.reorder_tsv(tsv, options = {})
    fields, undirected, persist = Misc.process_options options, :fields, :undirected, :persist 
    fields = tsv.fields if fields.nil?
    all_fields = tsv.all_fields

    source_pos, field_pos, source_header, field_headers, source_format, target_format = headers(all_fields, fields, options)

    source_field = source_pos == :key ? :key : all_fields[source_pos]
    info_fields = field_pos.collect{|f| f == :key ? :key : all_fields[f]}
    options = options.merge({:key_field => source_field, :fields =>  info_fields})

    tsv = tsv.reorder source_field, fields

    tsv.key_field = source_header
    tsv.fields = field_headers

    tsv = translate tsv, source_format, target_format, :persist => persist if source_format or target_format

    tsv = add_reciprocal tsv if undirected

    tsv
  end

  def self.open_stream(stream, options = {})
    fields, undirected, persist = Misc.process_options options, :fields, :undirected, :persist

    parser = TSV::Parser.new stream, options.merge(:fields => nil, :key_field => nil)

    key_field, *_fields = all_fields = parser.all_fields
    fields = _fields if fields.nil?

    source_pos, field_pos, source_header, field_headers, source_format, target_format = headers parser.all_fields, fields, options

    parser.key_field = source_pos
    parser.fields = field_pos

    case parser.type
    when :double, :list, :single
      class << parser
        def get_values(parts)
          [parts[@key_field].split(@sep2,-1), parts.values_at(*@fields).collect{|v| v.split(@sep2,-1) }]
        end
      end
    when :flat
      class << parser
        def get_values(parts)
          fields = (0..parts.length-1).to_a - [@key_field]
          values = parts.values_at(*fields).compact.collect{|v| v.split(@sep2,-1) }.flatten
          [parts[@key_field].split(@sep2,-1), values]
        end
      end
    end

    open_options = options.merge(parser.options).merge(:parser => parser)

    tsv = TSV.parse parser.stream, {}, open_options
    tsv.key_field = source_header
    tsv.fields = field_headers

    tsv = tsv.to_double unless tsv.type == :double

    tsv = translate tsv, source_format, target_format, :persist => persist if source_format or target_format

    tsv = add_reciprocal tsv if undirected

    tsv
  end

  def self.database(file,  options = {})
    case file
    when TSV
      file = file.to_double unless file.type == :double
      reorder_tsv(file, options.dup)
    when IO
      open_stream(file, options.dup)
    else
      stream = TSV.get_stream(file)
      open_stream(stream, options.dup)
    end
  end
  
end
