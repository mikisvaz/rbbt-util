require 'rbbt/entity'

module Association
  def self.identify_entity_format(format, fields)
    entity_type = Entity.formats[format]
    raise "Field #{ format } could not be resolved: #{fields}" if entity_type.nil?
    main_field = fields.select{|f| Entity.formats[f] == entity_type}.first
    raise "Field #{ format } not present, options: #{Misc.fingerprint fields}" if main_field.nil?
    [main_field, nil, format]
  end

  def self.parse_field_specification(spec)
    return [2,nil,nil] if Fixnum === spec
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
    source, source_format, target, target_format = Misc.process_options options, :source, :source_format, :target, :target_format

    key_field, *fields = all_fields.nil? ? [nil] : all_fields

    source_specs = normalize_specs  source, all_fields
    target_specs = normalize_specs  target, all_fields

    source_specs = [nil, nil, nil] if source_specs.nil?
    target_specs = [nil, nil, nil] if target_specs.nil?

    source_specs[2] = source_format if source_format
    target_specs[2] = target_format if target_format

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

    source_pos = all_fields.index source_field
    target_pos = all_fields.index target_field

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
               when Fixnum
                 all_fields[field] 
               when :key
                 all_fields.first
               end


      field_headers << header
    end

    field_pos = info_fields.collect{|f| raise "Field #{f} not found. Options: #{info_fields* ", "}" unless all_fields.include?(f); f == :key ? 0 : all_fields.index(f);  }

    source_format = specs[:source][2]
    target_format = specs[:target][2]


    if format = options[:format]
      source_format = process_formats(specs[:source][1] || specs[:source][0], format) || source_format
      target_format = process_formats(specs[:target][1] || specs[:target][0], format) || target_format
    end

    Log.low "Headers -- #{[source_pos, field_pos, source_header, field_headers, source_format, target_format]}"
    [source_pos, field_pos, source_header, field_headers, source_format, target_format]
  end
end
