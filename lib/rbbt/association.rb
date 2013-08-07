require 'rbbt-util'

module Association
  class << self
    attr_accessor :databases
    def databases
      @databases ||= {}
    end
  end

  def self.register(database, file, options = {})
    self.databases[database.to_s] = [file, options]
  end

  def self.get_database(database)
    self.databases[database.to_s]
  end

  def self.open_database(database, options = {}, persist_options = {})
    file, database_options = get_database database
    open(file, database_options.merge(options), persist_options)
  end

  def self.index_database(database, options = {}, persist_options = {})
    file, database_options = databases[database.to_s]
    index(file, database_options.merge(options), persist_options)
  end

  def self.parse_field_specification(spec, fields)
    spec = spec.split "=>" unless Array === spec
    field_part, final_type = spec

    field, type = field_part.split "=~"

    [field, type, final_type]
  end

  def self.resolve_field(name, fields)
    type = Entity.formats[name]
    return "Field #{ name } could not be resolved: #{fields}" if type.nil?
    field = fields.select{|f| Entity.formats[f] == type}.first
    [field, nil, name]
  end

  def self.add_reciprocal(tsv)
    new_tsv = {}
    tsv.with_unnamed do

      tsv.through do |target,v|
        source_values = tsv.type == :double ? Misc.zip_fields(v) : [v]
        
        source_values.each do |values|
          source = values.shift
          values.unshift target
          current = new_tsv[source] || tsv[source]

          case tsv.type
          when :double
            new  = current ? current.zip(values).collect{|p| p.flatten} : values.collect{|p| [p]}
          when :flat
            new = current ? (current + values).compact.uniq : values
          end

          new_tsv[source] = new
        end
      end

      tsv.merge! new_tsv
    end

    tsv
  end

  def self.load_tsv(file, options)
    key_field = TSV.parse_header(file, options).key_field
    fields = TSV.parse_header(file, options).fields
    all_fields = TSV.parse_header(file, options).all_fields
    
    source = options[:source] || options[:source_type]
    source = TSV.identify_field key_field, fields, options[:key_field] if source.nil? and options[:key_field]
    source = all_fields[source] if Fixnum === source
    source = key_field if source == :key or source.nil?

    target = options[:target]
    target = TSV.identify_field key_field, fields, options[:fields].first if target.nil? and options[:fields]
    target = all_fields[target] if Fixnum === target
    target = key_field if target == :key

    zipped = options[:zipped]
    undirected = options[:undirected]

    source, source_header, source_final_type = parse_field_specification source, all_fields
    target, target_header, target_final_type = parse_field_specification target, all_fields if target

    if source and not all_fields.include? source
      Log.debug("Resolving source: #{ source }")
      source, source_header, source_final_type = resolve_field source, all_fields
      Log.debug([source, source_header, source_final_type] * ", ")
    end

    if target and not all_fields.include? target
      Log.debug("Resolving target: #{ target }")
      target, target_header, target_final_type = resolve_field target, all_fields
      Log.debug([target, target_header, target_final_type] * ", ")
    end

    source_final_type ||= options[:source_type] if options[:source_type]
    target_final_type ||= options[:target_type] if options[:target_type]

    Log.debug("Loading associations from: #{ file }")
    Log.debug("sources: #{[source, source_header, source_final_type] * ", "}")
    Log.debug("targets: #{[target, target_header, target_final_type] * ", "}")
    if source != all_fields.first or (target and target != all_fields[1])
      fields = ([target] + (all_fields - [source, target])).compact
      open_options = options.merge({:key_field => source, :fields => fields})
      tsv = TSV.open(file, open_options)
    else
      tsv = TSV.open(file, options)
    end

    if source_header and tsv.key_field != source_header
      tsv.key_field = source_header
    end

    if source_final_type and tsv.key_field != source_final_type
      Log.debug("Changing source type from #{tsv.key_field} to #{source_final_type}")
      tsv.with_unnamed do
        tsv = TSVWorkflow.job(:change_id, tsv.filename, :tsv => tsv, :format => source_final_type, :organism => tsv.namespace).exec
      end
    end

    if target_header and tsv.fields.first != target_header
      tsv.fields = tsv.fields.collect{|f| f == target ? target_header : f }
    end

    if target_final_type and tsv.fields.first != target_final_type and
      Entity.formats[tsv.fields.first] and
      Entity.formats[tsv.fields.first] == Entity.formats[target_final_type]

      Log.debug("Changing target type from #{tsv.fields.first} to #{source_final_type}")
      save_key_field = tsv.key_field
      tsv.key_field = "MASKED"
      tsv.with_unnamed do
        tsv = TSVWorkflow.job(:swap_id, tsv.filename, :tsv => tsv, :field => tsv.fields.first, :format => target_final_type, :organism => tsv.namespace).exec
      end
      tsv.key_field = save_key_field 
    end

    if undirected
      tsv = add_reciprocal tsv
    end

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
      data.serializer = tsv.type if TokyoCabinet::HDB === data
      data.merge! tsv
      tsv.annotate data

      data
    end
  end

  def self.index(file, options = {}, persist_options = {})
    options = {} if options.nil?
    persist_options = {} if persist_options.nil?

    Persist.persist_tsv(file, nil, options, {:persist => true, :prefix => "Association Index"}.merge(persist_options).merge(:engine => TokyoCabinet::BDB, :serializer => :clean)) do |assocs|
      undirected = options[:undirected]
      tsv = TSV === file ? file : Association.open(file, options, persist_options.merge(:persist => false))

      key_field = [tsv.key_field, tsv.fields.first.split(":").last, undirected ? "undirected" : nil].compact * "~"

      TSV.setup(assocs, :key_field => key_field, :fields => tsv.fields[1..-1], :type => :list, :serializer => :list)

      tsv.with_unnamed do
        tsv.with_monitor :desc => "Extracting annotations" do
          case tsv.type
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
              next if source.nil?
              next if values.empty?
              targets = values.first
              rest = Misc.zip_fields values[1..-1]

              annotations = rest.length > 1 ?
                targets.zip(rest) :
                targets.zip(rest * targets.length) 

              annotations.each do |target, info|
                next if target.nil?
                key = [source, target] * "~"
                assocs[key] = info
              end
            end
          else
            raise "Type not supported: #{tsv.type}"
          end
        end
      end
      assocs.close

      assocs
    end
  end

  def self.connections(repo, entities)
    source_field, target_field, undirected = repo.key_field.split("~")

    source_type = Entity.formats[source_field].to_s
    target_type = Entity.formats[target_field].to_s

    source_entities = entities[source_type] || entities[source_field]
    target_entities = entities[target_type] || entities[target_field]

    return [] if source_entities.nil? or target_entities.nil?

    source_entities.collect do |entity|
      keys = repo.prefix(entity + "~")
      keys.collect do |key|
        source, target = key.split("~")
        next unless target_entities.include? target
        next if undirected and target > source
        info = Hash[*repo.fields.zip(repo[key]).flatten]

        {:source => source, :target => target, :info => info}
      end.compact
    end.flatten
  end
end
