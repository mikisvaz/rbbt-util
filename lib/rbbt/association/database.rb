require 'rbbt/association/util'
require 'rbbt/tsv/change_id'

module Association

  def self.add_reciprocal(tsv)
    new = TSV.open(tsv.dumper_stream)
    tsv.with_unnamed do
      case tsv.type
      when :double
        tsv.through do |source, values|
          Misc.zip_fields(values).each do |info|
            target, *rest = info
            next if target == source
            rest.unshift source
            new.zip_new target, rest
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
    namespace = tsv.namespace

    if source_final_format and source_field != source_final_format 
      Log.debug("Changing source format from #{tsv.key_field} to #{source_final_format}")

      identifier_files = tsv.identifier_files.dup
      identifier_files = [Organism.identifiers("NAMESPACE")] if identifier_files.empty?
      identifier_files.concat Entity.identifier_files(source_final_format) if defined? Entity
      identifier_files.uniq!
      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if namespace
      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}

      tsv = TSV.translate(tsv, source_field, source_final_format, options.merge(:identifier_files => identifier_files))
    end

    # Translate target 
    if target_final_format and target_field != target_final_format
      Log.debug("Changing target format from #{target_field} to #{target_final_format}")
      old_key_field = tsv.key_field 
      tsv.key_field = "MASK"

      identifier_files = tsv.identifier_files.dup 
      identifier_files.concat Entity.identifier_files(target_final_format) if defined? Entity
      identifier_files.uniq!
      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if namespace
      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}

      tsv = TSV.translate(tsv, target_field, target_final_format, options.merge(:identifier_files => identifier_files))
      tsv.key_field = old_key_field
    end

    tsv
  end

  def self.reorder_tsv(tsv, options = {})
    fields, persist = Misc.process_options options, :fields, :persist 
    all_fields = tsv.all_fields

    source_pos, field_pos, source_header, field_headers, source_format, target_format = headers(all_fields, fields, options)

    source_field = source_pos == :key ? :key : all_fields[source_pos]
    info_fields = field_pos.collect{|f| f == :key ? :key : all_fields[f]}
    options = options.merge({:key_field => source_field, :fields =>  info_fields})

    tsv.with_monitor(options[:monitor]) do
      tsv = tsv.reorder source_field, fields if true or source_field != tsv.key_field or (fields and tsv.fields != fields)
    end

    tsv.key_field = source_header
    tsv.fields = field_headers

    tsv = translate tsv, source_format, target_format, :persist => persist if source_format or target_format

    tsv
  end

  def self.open_stream(stream, options = {})
    fields, persist = Misc.process_options options, :fields, :persist

    parser = TSV::Parser.new stream, options.merge(:fields => nil, :key_field => nil)

    key_field, *_fields = all_fields = parser.all_fields

    source_pos, field_pos, source_header, field_headers, source_format, target_format = headers parser.all_fields, fields, options

    parser.key_field = source_pos
    parser.fields = field_pos

    case parser.type
    when :single
      class << parser
        def get_values(parts)
          [parts[@key_field], parts.values_at(*@fields).first]
        end
      end
    when :list
      class << parser
        def get_values(parts)
          [parts[@key_field], parts.values_at(*@fields)]
        end
      end
    when :double, :list, :single
      class << parser
        def get_values(parts)
          [parts[@key_field].split(@sep2,-1), parts.values_at(*@fields).collect{|v| v.nil? ? [] : v.split(@sep2,-1) }]
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
    open_options = Misc.add_defaults open_options, :monitor => {:desc => "Parsing #{ Misc.fingerprint stream }"}

    tsv = TSV.parse parser.stream, {}, open_options
    tsv.key_field = source_header
    tsv.fields = field_headers

    tsv = tsv.to_double unless tsv.type == :double

    tsv = translate tsv, source_format, target_format, :persist => persist if source_format or target_format

    tsv
  end

  def self.database(file,  options = {})
    database = case file
               when (defined? Step and Step)
                 file.clean if file.error? or file.aborted? or file.dirty?
                 file.run(true) unless file.done? or file.started?
                 file.join unless file.done?
                 open_stream(TSV.get_stream(file), options.dup)
               when TSV
                 file = file.to_double unless file.type == :double
                 reorder_tsv(file, options.dup)
               when IO
                 open_stream(file, options.dup)
               else
                 stream = TSV.get_stream(file)
                 open_stream(stream, options.dup)
               end

    database.entity_options = options[:entity_options] if options[:entity_options]

    database
  end
  
end
