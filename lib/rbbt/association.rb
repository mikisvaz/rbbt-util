require 'rbbt-util'
require 'rbbt/tsv/change_id'
require 'rbbt/association/index'

module Association
  class << self
    attr_accessor :databases
    def databases
      @databases ||= {}
    end
  end

  def self.add_reciprocal(tsv)
    tsv = tsv.type == :double ? tsv : tsv.to_double
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

  def self.resolve_field(name, fields)
    entity_type = Entity.formats[name]
    return "Field #{ name } could not be resolved: #{fields}" if entity_type.nil?
    field = fields.select{|f| Entity.formats[f] == entity_type}.first
    [field, nil, name]
  end

  def self.parse_field_specification(spec)
    return [2,nil,nil] if Fixnum === spec
    spec = spec.split "=>" unless Array === spec
    field_part, final_format = spec

    field, format = field_part.split "=~"

    [field, format, final_format]
  end

  def self.calculate_headers(key_field, fields, spec)
    all_fields = [key_field].concat fields if fields and key_field
    field, header, format = parse_field_specification spec if spec

    if field and key_field == field and not all_fields.include? field
      field, header, format = resolve_field field, all_fields
    end

    [field, header, format]
  end

  #{{{ Open

  def self.open_tsv(file, source, source_header, target, target_header, all_fields, options)
    fields = Misc.process_options options, :fields
    fields ||= all_fields.dup

    fields.delete source 
    fields.delete target
    fields.unshift target 

    open_options = options.merge({
      :persist => false,
      :key_field => all_fields.index(source), 
      :fields => fields.collect{|f| String === f ? all_fields.index(f): f },
      :unnamed => true,
      :merge => (options[:merge] or (options[:type] and options[:type].to_sym == :flat) ? false : true)
    })

    open_options["header_hash"] = "#" if options["header_hash"] == ""

    field_headers = all_fields.values_at *open_options[:fields]

    tsv = case file
          when TSV
            if file.fields == field_headers
              file
            else
              file.reorder(source, field_headers)
            end
          else
            TSV.open(file, open_options)
          end

    tsv.fields = field_headers
    tsv.key_field = source

    # Fix source header
    if source_header and tsv.key_field != source_header
      tsv.key_field = source_header
    end

    # Fix target header
    if target_header and tsv.fields.first != target_header
      tsv.fields = tsv.fields.collect{|f| f == target ? target_header : f }
    end

    tsv
  end

  def self.translate_tsv(tsv, source_final_format, target_final_format)
    source_field = tsv.key_field
    target_field = tsv.fields.first

    if source_final_format and source_field != source_final_format and
      Entity.formats[source_field] and
      Entity.formats[source_final_format].all_formats.include? source_field
      Log.debug("Changing source format from #{tsv.key_field} to #{source_final_format}")

      tsv.with_unnamed do
        tsv = tsv.change_key source_final_format, :identifiers => Organism.identifiers(tsv.namespace), :persist => true
      end
    end


    # Translate target 
    if target_final_format and target_field != target_final_format and
      Entity.formats[target_field] and
      Entity.formats[target_field] == Entity.formats[target_final_format]

      Log.debug("Changing target format from #{tsv.fields.first} to #{target_final_format}")

      save_key_field = tsv.key_field
      tsv.key_field = "MASKED"

      tsv.with_unnamed do
        tsv = tsv.swap_id tsv.fields.first, target_final_format, :identifiers => Organism.identifiers(tsv.namespace), :persist => true, :compact => true
      end

      tsv.key_field = save_key_field 
    end

    tsv
  end

  def self.specs(all_fields, options = {})
    source_spec, source_format, target_spec, target_format, format, key_field, fields = Misc.process_options options, :source, :source_format, :target, :target_format, :format, :key_field, :fields

    if key_field and all_fields
      key_pos = (Fixnum === key_field ? key_field : all_fields.index(key_field) )
      key_field = all_fields[key_pos]
    else
      key_field = all_fields.first if all_fields
    end

    if fields and all_fields
      field_pos = fields.collect{|f| Fixnum === f ? f : all_fields.index(f) }
      fields = all_fields.values_at *field_pos
    else
      #fields = all_fields[1..-1] if all_fields
    end

    source, source_header, orig_source_format = calculate_headers(key_field, fields || all_fields, source_spec)
    source_format ||= orig_source_format 
    source = key_field if source.nil? 
    source = key_field if source == :key
    source_header ||= source

    target, target_header, orig_target_format = calculate_headers(key_field, fields || all_fields, target_spec)
    target_format ||= orig_target_format 
    if target.nil?
      target = case
               when fields
                 fields.first
               when key_field == source
                 all_fields[1]
               else
                 (([key_field] + all_fields) - [source]).first
               end
    end

    target = key_field if target == :key
    target_header ||= target

    case format
    when String
      source_format ||= format if Entity.formats[source_header] == Entity.formats[format]
      target_format ||= format if Entity.formats[target_header] == Entity.formats[format]
    when Hash
      _type = Entity.formats[source_header].to_s
      source_format ||= format[_type] if format.include? _type 
      _type = Entity.formats[target_header].to_s
      target_format ||= format[_type] if format.include? _type 
    end

    [source, source_header, source_format, target, target_header, target_format, fields || all_fields]
  end

  def self.load_tsv(file, options)
    undirected = options[:undirected]

    case file
    when Proc
      return load_tsv(file.call, options)
    when TSV
      key_field, *fields = all_fields = file.all_fields
    else 
      key_field, *fields = all_fields = TSV.parse_header(file, options.merge(:fields => nil, :key_field => nil)).all_fields
    end

    source, source_header, source_format, target, target_header, target_format, fields = specs(all_fields, options)
 
    Log.low("Loading associations from: #{ Misc.fingerprint file }")
    Log.low("sources: #{ [source, source_header, source_format].join(", ") }")
    Log.low("targets: #{ [target, target_header, target_format].join(", ") }")

    tsv = open_tsv(file, source, source_header, target, target_header, all_fields, options.merge(:fields => fields.dup))

    tsv = translate_tsv(tsv, source_format, target_format)

    tsv = add_reciprocal(tsv) if undirected

    tsv
  end

  def self.open(file, options = {}, persist_options = {})
    options = {} if options.nil?
    persist_options = {} if persist_options.nil?

    namespace = options[:namespace]
    old_file, file = file, file.sub('NAMESPACE', namespace) if namespace and String === file
    old_file.annotate file if Path === old_file

    Persist.persist_tsv(file, nil, options, {:persist => true, :prefix => "Association"}.merge(persist_options)) do |data|
      options = options.clone

      tsv = load_tsv(file, options)

      tsv.annotate(data)
      data.serializer = tsv.type 

      tsv.with_unnamed do
        tsv.each do |k,v|
          next if v.nil?
          data[k] = v
        end
      end

      data
    end
  end

  #{{{ Index

  #def self.get_index(index_file, write = false)
  #  Persist.open_tokyocabinet(index_file, write, :list, TokyoCabinet::BDB).tap{|r| r.unnamed = true; Association::Index.setup r }
  #end

  def self.index(file, options = {}, persist_options = nil)
    options = {} if options.nil?
    options = Misc.add_defaults options, :persist => true, :undirected => false
    persist_options = Misc.pull_keys options, persist_options if persist_options.nil?

    Persist.persist_tsv(file, nil, options, {:persist => true, :prefix => "Association Index"}.merge(persist_options).merge(:engine => TokyoCabinet::BDB, :serializer => :clean)) do |assocs|
      undirected = options[:undirected]
      if file
        tsv = TSV === file ? file : Association.open(file, options, persist_options.merge(:persist => false))

        fields = tsv.fields
        source_field = tsv.key_field
        target_field = fields.first.split(":").last

        key_field = [source_field, target_field, undirected ? "undirected" : nil].compact * "~"

        TSV.setup(assocs, :key_field => key_field, :fields => fields[1..-1], :type => :list, :serializer => :list)

        tsv.with_unnamed do
          tsv.with_monitor :desc => "Extracting associations" do
            case tsv.type
            when :list
              tsv.through do |source, values|
                target, *rest = values
                next if source.nil? or source.empty? or target.nil? or target.empty?

                key = [source, target] * "~"
                assocs[key] = rest
              end
            when :flat
              tsv.through do |source, targets|
                next if source.nil? or source.empty? or targets.nil? or targets.empty?

                targets.each do |target|
                  next if target.nil? or target.empty?
                  key = [source, target] * "~"
                  assocs[key] = nil
                end
              end

            when :double
              tsv.through do |source, values|
                next if values.empty?
                next if source.nil? or source.empty?
                next if values.empty?
                targets = values.first
                rest = Misc.zip_fields values[1..-1]

                annotations = rest.length > 1 ?
                  targets.zip(rest) :
                  targets.zip(rest * targets.length) 

                annotations.each do |target, info|
                  next if target.nil? or target.empty?
                  key = [source, target] * "~"
                  if assocs[key].nil? or info.nil?
                    assocs[key] = info
                  else
                    old_info = assocs[key]
                    info = old_info.zip(info).collect{|p| p * ";;" }
                    assocs[key] = info
                  end
                end
              end
            else
              raise "Type not supported: #{tsv.type}"
            end
          end
        end
      else
        key_field, fields = options.values_at :key_field, :fields
        TSV.setup(assocs, :key_field => key_field, :fields => fields[1..-1], :type => :list, :serializer => :list)
      end
      assocs.close

      assocs
    end.tap do |assocs|
      Association::Index.setup assocs
    end
  end
end
