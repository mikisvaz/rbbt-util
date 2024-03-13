require 'rbbt/entity'

module Association
  def self.identify_entity_format(format, fields)
    entity_type = Entity.formats[format]
    raise "Field #{ format } could not be resolved: #{fields}" if entity_type.nil?
    main_field = fields.select{|f| Entity.formats[f] == entity_type}.first
    raise "Field #{ format } not present, options: #{Log.fingerprint fields}" if main_field.nil?
    [main_field, nil, format]
  end

  def self.parse_field_specification(spec)
    return [2,nil,nil] if Numeric === spec
    spec = spec.split "=>" unless Array === spec
    field_part, final_format = spec

    field, format = field_part.split "=~", -1

    field = nil if field.nil? or field.empty?

    [field, format, final_format]
  end

  def self.normalize_specs(spec, all_fields = nil)
    return nil if spec.nil?
    field, header, format = parse_field_specification spec 

    specs = if all_fields.nil? or all_fields.include? field
               [field, header, format]
             else
               if all_fields.nil?
                 begin
                   identify_entity_format field, all_fields 
                 rescue
                   [field, header, format]
                 end
               else
                 [field, header, format]
               end
             end
    specs
  end

  def self.extract_specs(all_fields=nil, options = {})
    source, source_format, target, target_format, format = IndiferentHash.process_options options, :source, :source_format, :target, :target_format, :format

    key_field, *fields = all_fields.nil? ? [nil] : all_fields

    source_specs = normalize_specs  source, all_fields
    target_specs = normalize_specs  target, all_fields

    source_specs = [nil, nil, nil] if source_specs.nil?
    target_specs = [nil, nil, nil] if target_specs.nil?

    source_specs[2] = source_format if source_format
    target_specs[2] = target_format if target_format

    if source_specs.first and not all_fields.include? source_specs.first and defined? Entity and (_format = Entity.formats[source_specs.first.to_s])
      _source = all_fields.select{|f| Entity.formats[f.to_s] == _format }.first
      raise "Source not found #{source_specs}. Options: #{Log.fingerprint all_fields}" if _source.nil?
      source_specs[0] = _source
    end

    if target_specs.first and  not all_fields.include? target_specs.first and defined? Entity and (_format = Entity.formats[target_specs.first.to_s])
      _target = all_fields.select{|f| Entity.formats[f.to_s].to_s == _format.to_s }.first
      raise "Target not found #{target_specs}. Options: #{Log.fingerprint all_fields}" if _target.nil?
      target_specs[0] = _target
    end

    if source_specs[0].nil? and target_specs[0].nil?
      source_specs[0] = key_field 
      target_specs[0] = fields[0]
    elsif source_specs[0].nil? 
      if target_specs[0] == :key or target_specs[0] == key_field
        source_specs[0] = fields[0]
      else
        source_specs[0] = key_field
      end
    elsif target_specs[0].nil? 
      if source_specs[0] == fields.first 
        target_specs[0] = key_field
      else
        target_specs[0] = fields.first 
      end
    end

    # If format is specified, then perhaps we need to change the
    if target_specs[2].nil? 
      target_type = Entity.formats[target_specs[1] || target_specs[0]]
      target_specs[2] = format[target_type.to_s] if format
      target_specs[2] = nil if target_specs[2] == target_specs[0] or target_specs[2] == target_specs[1]
    end

    if source_specs[2].nil? 
      source_type = Entity.formats[source_specs[1] || source_specs[0]]
      source_specs[2] = format[source_type.to_s] if format
      source_specs[2] = nil if source_specs[2] == source_specs[0] or source_specs[2] == source_specs[1]
    end

    {:source => source_specs, :target => target_specs}
  end

  def self.process_formats(field, default_format = {})
    return nil if default_format.nil? or default_format.empty?
    default_format.each do |type, format|
      entity_type = Entity.formats[field] || format
      return format if entity_type.to_s === type 
    end
    return nil
  end

  def self.headers(all_fields, info_fields = nil, options = {})
    specs = extract_specs all_fields, options

    source_field = specs[:source][0]
    target_field = specs[:target][0]

    #source_pos = all_fields.index source_field
    #target_pos = all_fields.index target_field
   
    source_pos = TSV.identify_field all_fields.first, all_fields[1..-1], source_field
    target_pos = TSV.identify_field all_fields.first, all_fields[1..-1], target_field

    source_pos = source_pos == :key ? 0 : source_pos + 1
    target_pos = target_pos == :key ? 0 : target_pos + 1

    source_header = specs[:source][1] || specs[:source][0]
    target_header = specs[:target][1] || specs[:target][0]

    info_fields = all_fields.dup if info_fields.nil?
    info_fields.delete source_field
    info_fields.delete target_field
    info_fields.unshift target_field

    field_headers = [target_header] 
    info_fields[1..-1].each do |field|
      header = case field
               when String 
                 field
               when Numeric
                 all_fields[field] 
               when :key
                 all_fields.first
               end

      field_headers << header
    end

    field_pos = info_fields.collect do |f| 
      p = TSV.identify_field all_fields.first, all_fields[1..-1], f
      p == :key ? 0 : p + 1
    end

    field_pos.delete source_pos

    source_format = specs[:source][2]
    target_format = specs[:target][2]


    if format = options[:format]
      source_format = process_formats(specs[:source][1] || specs[:source][0], format) || source_format unless source_format
      target_format = process_formats(specs[:target][1] || specs[:target][0], format) || target_format unless target_format
    end

    res = [source_pos, field_pos, source_header, field_headers, source_format, target_format]
    Log.low "Headers -- #{res}"
    res
  end
end
