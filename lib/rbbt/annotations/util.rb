require 'json'

module Annotated

  def self.load(object, info)
    annotation_types = info.delete(:annotation_types) || info.delete("annotation_types") || []
    annotation_types = annotation_types.split("|") if String === annotation_types

    return object if annotation_types.nil? or annotation_types.empty?

    annotated_array = false
    annotated_array = true if (info.delete(:annotated_array) || info.delete("annotated_array")).to_s == "true"
    entity_id = info.delete(:entity_id) || info.delete("entity_id")

    annotation_types.each do |mod|
      mod = Misc.string2const(mod) if String === mod
      object.extend mod unless mod === object
    end

    object.instance_variable_set(:@annotation_values, info) 

    object.instance_variable_set(:@id, entity_id) if entity_id

    object.extend AnnotatedArray if annotated_array

    object
  end

  def self.resolve_array(entry)
    if entry =~ /^Array:/
      entry["Array:".length..-1].split("|")
    else
      entry
    end
  end

  def self.load_tsv_values(id, values, *fields)
    fields = fields.flatten
    info = {}
    literal_pos = fields.index "literal"

    object = case
             when literal_pos
               values[literal_pos]
             else
               id.dup
             end

    object = resolve_array(object)

    if Array === values.first
      Misc.zip_fields(values).collect do |list|
        fields.each_with_index do |field,i|
          next if field == "literal"
          if field == "JSON"
            JSON.parse(list[i]).each do |key, value|
              info[key.to_sym] = value
            end
          else
            info[field.to_sym] = resolve_array(list[i])
          end
        end
      end
    else
      fields.each_with_index do |field,i|
        next if field == "literal"
        if field == "JSON"
          JSON.parse(values[i]).each do |key, value|
            info[key.to_sym] = value
          end
        else
          info[field.to_sym] = resolve_array(values[i])
        end
      end
    end

    self.load(object, info)

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

             when ((fields.compact.empty?) and not annotations.empty?)
               fields = AnnotatedArray === annotations ? annotations.annotations : annotations.compact.first.annotations
               fields << :annotation_types

             when (fields == [:literal] and not annotations.empty?)
               fields << :literal

             when (fields == [:all] and Annotated === annotations)
               fields = [:annotation_types] + annotations.annotations 
               fields << :annotated_array if AnnotatedArray === annotations
               fields << :literal

             when (fields == [:all] and not annotations.empty?)
               raise "Input array must be annotated or its elements must be" if not Annotated === annotations.compact.first and not Array === annotations.compact.first
               raise "Input array must be annotated or its elements must be. No duble arrays of singly annotated entities." if not Annotated === annotations.compact.first and Array === annotations.compact.first
               fields = [:annotation_types] + (Annotated === annotations ? 
                                               annotations.annotations: 
                                               annotations.compact.first.annotations)
               fields << :literal

             when annotations.empty?
               [:annotation_types, :literal]

             else
               fields.flatten

             end

    fields = fields.collect{|f| f.to_s}

    case

    when (Annotated === annotations and not (AnnotatedArray === annotations and annotations.double_array))
      tsv = TSV.setup({}, :key_field => "List", :fields => fields, :type => :list, :unnamed => true)

      annot_id = annotations.id
      tsv[annot_id] = annotations.tsv_values(*fields).dup

    when Array === annotations 
      tsv = TSV.setup({}, :key_field => "ID", :fields => fields, :type => :list, :unnamed => true)

      annotations.compact.each_with_index do |annotation,i|
        tsv[annotation.id + ":" << i.to_s] = annotation.tsv_values(*fields).dup
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


  end

