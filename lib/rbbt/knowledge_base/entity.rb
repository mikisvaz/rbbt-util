require 'rbbt/entity'
require 'rbbt/knowledge_base/registry'

class KnowledgeBase

  def select_entities(name, entities, options = {})
    index = get_index(name, options)
    source_field = index.source_field
    target_field = index.target_field

    source_type = Entity.formats[source_field] 
    target_type = Entity.formats[target_field]

    source_entities = entities[:source] || entities[source_field] || entities[Entity.formats[source_field].to_s] 
    target_entities = entities[:target] || entities[target_field] || entities[Entity.formats[target_field].to_s]

    [source_entities, target_entities]
  end


  def entity_options_for(type, database_name = nil)
    options = entity_options[Entity.formats[type]] || {}
    options[:format] = @format[type] if @format.include? :type
    options = {:organism => namespace}.merge(options)
    if database_name and 
      (database = get_database(database_name)).entity_options and
      (database = get_database(database_name)).entity_options[type]
      options = options.merge database.entity_options[type] 
    end
    options
  end

  def translate(entities, type)
    if format = @format[type] and (entities.respond_to? :format and format != entities.format)
      entities.to format
    else
      entities
    end
  end

  def annotate(entities, type, database = nil)
    format = @format[type] || type
    Misc.prepare_entity(entities, format, entity_options_for(type, database))
  end

  def source_type(name)
    Entity.formats[source(name)]
  end

  def target_type(name)
    Entity.formats[target(name)]
  end

  def entities
    all_databases.inject([]){|acc,name| acc << source(name); acc << target(name)}.uniq
  end

  def entity_types
    entities.collect{|entity| Entity.formats[entity] }.uniq
  end

  def identifier_files(name)
    get_database(name).identifier_files.dup
  end

  def source_index(name)
    Persist.memory("Source index #{name}: KB directory #{dir}") do
      identifier_files = identifier_files(name)
      identifier_files.concat Entity.identifier_files(source(name)) if defined? Entity
      identifier_files.uniq!
      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if namespace
      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}
      TSV.translation_index identifier_files, source(name), nil, :persist => true
    end
  end
  
  def target_index(name)
    Persist.memory("Target index #{name}: KB directory #{dir}") do
      identifier_files = identifier_files(name)
      identifier_files.concat Entity.identifier_files(source(name)) if defined? Entity
      identifier_files.uniq!
      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if namespace
      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}
      TSV.translation_index identifier_files, target(name), nil, :persist => true
    end
  end

  def identify_source(name, entity)
    return :all if entity == :all
    index = source_index(name)
    return entity if index.nil?
    Array === entity ? index.values_at(*entity) : index[entity]
  end

  
  def identify_target(name, entity)
    return :all if entity == :all
    index = target_index(name)
    return nil if index.nil?
    Array === entity ? index.values_at(*entity) : index[entity]
  end

  def identify(name, entity)
    identify_source(name, entity) || identify_target(name, entity)
  end
end

