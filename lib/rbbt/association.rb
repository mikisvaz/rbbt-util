require 'rbbt-util'

module Association
  class << self
    attr_accessor :databases
    def databases
      @databases ||= {}
    end
  end

  def self.open(file, options)
    options = Misc.add_defaults options, :merge => true
    namespace = options[:namespace]
    old_file, file = file, file.sub('NAMESPACE', namespace) if namespace and String === file
    old_file.annotate file if Path === old_file
    Persist.persist_tsv(file, nil, options, :persist => true, :prefix => "Association", :update => false) do |data|
      source = options.delete :source
      target = options.delete :target
      target_field = options.delete :target_field

      if source
        source_entity = Entity.formats[source]
        fields = TSV.parse_header(file, options).all_fields
        key_field = source if fields.include? source
        key_field = fields.select{|f| Entity.formats[f] == source_entity}.first if key_field.nil?
        options[:key_field] = key_field
      end

      if target
        target, target_field = target 
        fields = TSV.parse_header(file, options).all_fields
        if target_field.nil?
          target_entity = Entity.formats[target]
          key_field ||= TSV.parse_header(file, options).key_field
          target_field = target if fields.include? target
          target_field = fields.select{|f| Entity.formats[f] == target_entity}.first if main_field.nil?
        else
          target_field, target_type = target_field.split("=~")
        end
        options[:fields] = [target_field] + (fields - [key_field, target_field])
      end

      tsv = TSV.open file, options.merge(:persist => false)

      if source and tsv.key_field != source
        tsv.with_unnamed do
          tsv = TSVWorkflow.job(:change_id, file, :tsv => tsv, :format => source, :organism => namespace).exec
        end
      end

      tsv.fields = [target_type] + tsv.fields[1..-1] if target_type != target_field
      if target and tsv.fields.first != target
        tsv.with_unnamed do
          tsv = TSVWorkflow.job(:swap_id, file, :tsv => tsv, :field => tsv.fields.first, :format => target, :organism => namespace).exec
        end
      end

      tsv.fields = ["target:" + tsv.fields[0]] + tsv.fields[1..-1] if tsv.fields.first == tsv.key_field

      if options[:undirected]
        new_tsv = {}
        tsv.through do |target,v|
          v.zip_fields.each do |values|
            source = values.shift
            values.unshift target
            current = new_tsv[source] || tsv[source]
            if Array === current
              new = current.zip(values).collect{|p| p.flatten}
            else
              new = values.collect{|v| [v]}
            end
            #ddd "."
            #ddd source
            #ddd target
            #ddd current
            #ddd new
            new_tsv[source] = new
          end
        end
        tsv.merge! new_tsv
      end

      data.serializer = tsv.type if data.respond_to? :serializer
      data.merge! tsv
      tsv.annotate data

      data
    end
  end
end
