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
end

