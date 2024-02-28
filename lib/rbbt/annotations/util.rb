require 'json'

module Annotated

  def self.flatten(array)
    return array unless Array === array and not array.empty?
    array.extend AnnotatedArray if Annotated === array
    return array.flatten if AnnotatedArray === array
    begin
      return array if array.compact.collect{|e| e.info }.uniq.length > 1
    rescue
      return array
    end
    array.compact.first.annotate(array.flatten).tap{|a| a.extend AnnotatedArray }
  end

  def self.load_entity(object, info)
    annotation_types = info.delete(:annotation_types) || info.delete("annotation_types") || []
    annotation_types = annotation_types.split("|") if String === annotation_types

    return object if annotation_types.nil? or annotation_types.empty?

    annotated_array = false
    annotated_array = true if (info.delete(:annotated_array) || info.delete("annotated_array")).to_s == "true"
    entity_id = info.delete(:entity_id) || info.delete("entity_id")

    annotation_types.each do |mod|
      begin
        mod = Misc.string2const(mod) if String === mod
        object.extend mod unless mod === object
      rescue Exception
        Log.warn "Exception loading annotation into object: #{$!.message}"
      end
    end

    object.instance_variable_set(:@annotation_values, info) 

    object.instance_variable_set(:@id, entity_id) if entity_id

    object.extend AnnotatedArray if annotated_array and Array === object

    object
  end

  def self.resolve_array(entry)
    if String === entry && entry =~ /^Array:/
      entry["Array:".length..-1].split("|")
    else
      entry
    end
  end

  def self.load_info(fields, values)
    info = {}
    fields.each_with_index do |field,i|
      next if field == "literal"
      case field
      when "JSON"
        JSON.parse(values[i]).each do |key, value|
          info[key.to_sym] = value
        end
      when nil
        next
      else
        info[field.to_sym] = resolve_array(values[i])
      end
    end
    info
  end

  def self.load_tsv_values(id, values, *fields)
    fields = fields.flatten
    literal_pos = fields.index "literal"

    object = case
             when literal_pos
               values[literal_pos].tap{|o| o.force_encoding(Encoding.default_external)}
             else
               id.dup
             end

    object = resolve_array(object)

    if Array === values.first
      Misc.zip_fields(values).collect do |v|
        info = load_info(fields, v)
      end
    else
      info = load_info(fields, values)
    end

    self.load_entity(object, info)

    object
  end

  def self.load_tsv(tsv)
    tsv.with_unnamed do
      annotated_entities = tsv.collect do |id, values|
        Annotated.load_tsv_values(id, values, tsv.fields)
      end

      case tsv.key_field 
      when "List"
        annotated_entities.first
      else
        annotated_entities
      end
    end
  end


  def self.tsv(annotations, *fields)
    return nil if annotations.nil?

    fields = case

             when ((fields.compact.empty?) && ! annotations.empty?)
               fields = AnnotatedArray === annotations ? annotations.annotations : annotations.compact.first.annotations
               fields << :annotation_types

             when (fields == [:literal] and ! annotations.compact.empty?)
               fields << :literal

             when (fields == [:all] && Annotated === annotations)
               annotation_names = annotations.annotations
               annotation_names += annotations.first.annotations if Annotated === annotations.first
               fields = [:annotation_types] + annotation_names.uniq
               fields << :annotated_array if AnnotatedArray === annotations
               fields << :literal

             when (fields == [:all] && ! annotations.compact.empty?)
               raise "Input array must be annotated or its elements must be" if not Annotated === annotations.compact.first and not Array === annotations.compact.first
               raise "Input array must be annotated or its elements must be. No double arrays of singly annotated entities." if not Annotated === annotations.compact.first and Array === annotations.compact.first
               fields = [:annotation_types] + (Annotated === annotations ? 
                                               annotations.annotations: 
                                               annotations.compact.first.annotations)
               fields << :literal

             when annotations.empty?
               [:annotation_types, :literal]

             else
               fields.flatten

             end

    fields = fields.collect{|f| f.to_s}.uniq

    case
    when (Annotated === annotations and not (AnnotatedArray === annotations and annotations.double_array))
      tsv = TSV.setup({}, :key_field => "List", :fields => fields, :type => :list, :unnamed => true)

      annot_id = annotations.id
      annot_id = annot_id * "," if Array === annot_id
      tsv[annot_id] = annotations.tsv_values(*fields).dup

    when Array === annotations 
      tsv = TSV.setup({}, :key_field => "ID", :fields => fields, :type => :list, :unnamed => true)

      annotations.compact.each_with_index do |annotation,i|
        tsv[annotation.id + "#" << i.to_s] = annotation.tsv_values(*fields).dup
      end

    else
      raise "Annotations need to be an Array to create TSV"
    end

    tsv
  end

  def tsv_values(*fields)
    if Array === self and (not AnnotatedArray === self or self.double_array)
      Misc.zip_fields(self.compact.collect{|e| e.tsv_values(fields)})
    else
      fields = fields.flatten

      info = self.info

      values = []

      fields.each do |field|
        values << case

        when Proc === field
          field.call(self)

        when field == "JSON"
          if AnnotatedArray === self
            info.merge(:annotated_array => true).to_json
          else
            info.to_json
          end

        when field == "annotation_types"
          annotation_types.collect{|t| t.to_s} * "|"

        when field == "annotated_array"
          AnnotatedArray === self

        when field == "literal"
          (Array === self ? "Array:" << self * "|" : self).gsub(/\n|\t/, ' ')

        when info.include?(field.to_sym)
          res = info[field.to_sym]
          Array === res ? "Array:" << res * "|" : res

        when self.respond_to?(field)
          res = self.send(field)
          Array === res ? "Array:"<< res * "|" : res

        end
      end


      values
    end
  end

  def self.to_hash(e)
    hash = {}
    if Array === e && AnnotatedArray === e
      hash[:literal] = Annotated.purge(e)
      hash[:info] = e.info
    elsif Array === e
      hash = e.collect do |_e|
        _hash = {}
        _hash[:literal] = _e.dup
        _hash[:info] = _e.info if _e.respond_to?(:info)
        _hash
      end
    else
      hash[:literal] = e.dup
      hash[:info] = e.info
    end
    hash
  end

  def self.load_hash(hash)
    if Array === hash
      hash.collect{|h| load_hash(h) }
    else
      literal = hash[:literal]
      info = hash[:info]
      info[:annotation_types].each do |type|
        type = Kernel.const_get(type) if String === type
        type.setup(literal, info) 
      end
      literal
    end
  end

  def marshal_dump
    Annotated.to_hash(self)
  end
end

class String
  def marshal_load(hash)
    if Hash === hash
      e = Annotated.load_hash(hash)
      self.replace e
      e.annotate(self)
      self
    else
      self.replace hash
    end
  end
end

class Array
  def marshal_load(hash)
    if Hash === hash
      e = Annotated.load_hash(hash)
      self.replace e
      e.annotate(self)
      self
    else
      self.replace hash
    end
  end
end
