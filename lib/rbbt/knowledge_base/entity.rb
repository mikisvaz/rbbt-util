#require 'rbbt/entity'
#require 'rbbt/knowledge_base/registry'
#
#class KnowledgeBase
#
#  def select_entities(name, entities, options = {})
#    index = get_index(name, options)
#
#    source_field = index.source_field
#    target_field = index.target_field
#
#    source_type = Entity.formats[source_field] 
#    target_type = Entity.formats[target_field]
#
#    source_entities = entities[:source] || entities[source_field] || entities[Entity.formats[source_field].to_s] || entities[:both]
#    target_entities = entities[:target] || entities[target_field] || entities[Entity.formats[target_field].to_s] || entities[:both]
#
#    [source_entities, target_entities]
#  end
#
#
#  def entity_options_for(type, database_name = nil)
#    entity_options = self.entity_options
#    IndiferentHash.setup entity_options if entity_options and not IndiferentHash === entity_options
#    options = entity_options[type.to_s] || entity_options[Entity.formats[type.to_s].to_s] || {}
#    options[:format] = @format[type] if Hash === @format && @format.include?(type)
#    namespace = self.namespace
#    namespace = db_namespace(database_name) if namespace.nil? and database_name
#    options = {:organism => namespace}.merge(options)
#    if database_name  
#      database = get_database(database_name)
#      if database.entity_options and (database.entity_options[type] or database.entity_options[Entity.formats[type.to_s].to_s])
#        options = options.merge(database.entity_options[type] || database.entity_options[Entity.formats[type.to_s].to_s])
#      end
#    end
#    options
#  end
#
#  def annotate(entities, type, database = nil)
#    format = @format[type] || type
#    entity_options = entity_options_for(type, database)
#    Entity.prepare_entity(entities, format, entity_options)
#  end
#
#  def translate(entities, type)
#    if format = @format[type] and (entities.respond_to? :format and format != entities.format)
#      entities.to format
#    else
#      entities
#    end
#  end
#
#  def source_type(name)
#    Entity.formats[source(name)]
#  end
#
#  def target_type(name)
#    Entity.formats[target(name)]
#  end
#
#  def entities
#    all_databases.inject([]){|acc,name| acc << source(name); acc << target(name)}.uniq
#  end
#
#  def entity_types
#    entities.collect{|entity| Entity.formats[entity] }.uniq
#  end
#
#  def identifier_files(name)
#    get_database(name).identifier_files.dup
#  end
#
#  def db_namespace(name)
#    get_database(name).namespace
#  end
#
#  def source_index(name)
#    Persist.memory("Source index #{name}: KB directory #{dir}") do
#      identifier_files = identifier_files(name)
#      identifier_files.concat Entity.identifier_files(source(name)) if defined? Entity
#      identifier_files.uniq!
#      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if namespace
#      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, db_namespace(name)))} if not namespace and db_namespace(name)
#      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}
#      TSV.translation_index identifier_files, nil, source(name), :persist => true
#    end
#  end
#  
#  def target_index(name)
#    Persist.memory("Target index #{name}: KB directory #{dir}") do
#      identifier_files = identifier_files(name)
#      identifier_files.concat Entity.identifier_files(target(name)) if defined? Entity
#      identifier_files.uniq!
#      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, namespace))} if self.namespace
#      identifier_files.collect!{|f| f.annotate(f.gsub(/\bNAMESPACE\b/, db_namespace(name)))} if namespace.nil? and db_namespace(name)
#      identifier_files.reject!{|f| f.match(/\bNAMESPACE\b/)}
#      TSV.translation_index identifier_files, nil, target(name), :persist => true
#    end
#  end
#
#  def identify_source(name, entity)
#    return :all if entity == :all
#    index = source_index(name)
#    return entity if index.nil?
#    if Array === entity
#      entity.collect{|e| index[e] || e }
#    else
#      index[entity] || entity
#    end
#  end
#
#  
#  def identify_target(name, entity)
#    return :all if entity == :all
#    index = target_index(name)
#    return entity if index.nil?
#    if Array === entity
#      entity.collect{|e| index[e] || e }
#    else
#      index[entity] || entity
#    end
#  end
#
#  def identify(name, entity)
#    identify_source(name, entity) || identify_target(name, entity)
#  end
#end
#
