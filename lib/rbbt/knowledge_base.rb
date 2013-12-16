require 'rbbt/association'
require 'rbbt/association/item'
require 'rbbt/entity'

class KnowledgeBase
  class << self
    attr_accessor :knowledge_base_dir, :registry

    def registry
      @registry ||= IndiferentHash.setup({})
    end
    
    def knowledge_base_dir
      @knowledge_base_dir ||= Rbbt.var.knowledge_base
    end
  end

  attr_accessor :namespace, :dir, :indices, :registry, :format, :databases, :entity_options
  def initialize(dir, namespace = nil)
    @dir = Path.setup(dir).find

    @namespace = namespace
    @format = IndiferentHash.setup({})

    @registry = IndiferentHash.setup({})
    @entity_options = IndiferentHash.setup({})

    @indices = IndiferentHash.setup({})
    @databases = IndiferentHash.setup({})
    @identifiers = IndiferentHash.setup({})
    @descriptions = {}
    @databases = {}
  end

  def version(new_namespace, force = false)
    return self if new_namespace == namespace and not force
    new_kb = KnowledgeBase.new dir[new_namespace], new_namespace
    new_kb.format.merge! self.format
    new_kb.entity_options.merge! self.entity_options
    new_kb.registry = self.registry
    new_kb
  end

  #{{{ Descriptions
 
  def register(name, file = nil, options = {}, &block)
    if block_given?
      block.define_singleton_method(:filename) do name.to_s end
      Log.debug("Registering #{ name } from code block")
      @registry[name] = [block, options]
    else
      Log.debug("Registering #{ name }: #{ Misc.fingerprint file }")
      @registry[name] = [file, options]
    end
  end

  def syndicate(kb, name)
    kb.all_databases.each do |database|
      db_name = [database, name] * "@"
      file, kb_options = kb.registry[database]
      options = {}
      options[:undirected] = true if kb_options and kb_options[:undirected]
      register(db_name, nil, options) do
        kb.get_database(database)
      end
    end
  end

  def all_databases
    @registry.keys
  end

  def description(name)
    @descriptions[name] ||= get_index(name).key_field.split("~")
  end

  def source(name)
    description(name)[0]
  end

  def target(name)
    description(name)[1]
  end

  def undirected(name)
    description(name)[2]
  end

  def source_type(name)
    Entity.formats[source(name)]
  end

  def target_type(name)
    Entity.formats[target(name)]
  end

  def index_fields(name)
    get_index(name).fields
  end

  def entities
    all_databases.inject([]){|acc,name| acc << source(name); acc << target(name)}.uniq
  end

  def entity_types
    entities.collect{|entity| Entity.formats[entity] }.uniq
  end

  #{{{ Open and get
 
  def open_options
    {:namespace => namespace, :format => @format}
  end
 
  def get_database(name, options = {})
    @databases[Misc.fingerprint([name, options])] ||= \
      begin 
        Persist.memory("Database:" <<[name, self.dir] * "@") do
          options = Misc.add_defaults options, :persist_dir => dir.databases
          persist_options = Misc.pull_keys options, :persist

          file, registered_options = registry[name]
          options = open_options.merge(registered_options || {}).merge(options)
          raise "Repo #{ name } not found and not registered" if file.nil?

          Log.low "Opening database #{ name } from #{ Misc.fingerprint file }. #{options}"
          Association.open(file, options, persist_options).
            tap{|tsv| tsv.namespace = self.namespace}
        end
      end
  end


  def get_index(name, options = {})
    @indices[Misc.fingerprint([name, options])] ||= \
      begin 
        Persist.memory("Index:" <<[name, self.dir] * "@") do
          options = Misc.add_defaults options, :persist_dir => dir.indices
          persist_options = Misc.pull_keys options, :persist

          file, registered_options = registry[name]
          options = open_options.merge(registered_options || {}).merge(options)
          raise "Repo #{ name } not found and not registered" if file.nil?

          Log.low "Opening index #{ name } from #{ Misc.fingerprint file }. #{options}"
          Association.index(file, options, persist_options).
            tap{|tsv| tsv.namespace = self.namespace}
        end
      end
  end

  #def index(name, file, options = {}, persist_options = {})
  #  @indices[name] = Association.index(file, open_options.merge(options), persist_options)
  #end

  #{{{ Add manual database
  
  def add_index(name, source_type, target_type, *fields)
    options = fields.pop if Hash === fields.last
    options ||= {}
    undirected = Misc.process_options options, :undirected 

    undirected = nil unless undirected 

    repo_file = dir[name].find
    index = Association.index(nil, {:namespace => namespace, :key_field => [source_type, target_type, undirected].compact * "~", :fields => fields}.merge(options), :file => repo_file, :update => true)
    @indices[name] = index
  end

  def add(name, source, target, *rest)
    code = [source, target] * "~"
    repo = @indices[name]
    repo[code] = rest
  end

  def write(name)
    repo = @indices[name]
    repo.write_and_read do
      yield
    end
  end

  #{{{ Annotate
  
  def entity_options_for(type, database_name = nil)
    options = entity_options[Entity.formats[type]] || {}
    options[:format] = @format[type] if @format.include? :type
    options = {:organism => namespace}.merge(options)
    if database_name and (database = get_database(database_name)).entity_options
      options = options.merge database.entity_options
    end
    options
  end

  def annotate(entities, type, database = nil)
    Misc.prepare_entity(entities, type, entity_options_for(type, database))
  end

  #{{{ Identify
  
  def identify_source(name, entity)
    database = get_database(name, :persist => true)
    return entity if database.include? entity
    source = source(name)
    @identifiers[name] ||= {}
    @identifiers[name]['source'] ||= begin
                                       if database.identifier_files.any?
                                         if TSV.parse_header(database.identifier_files.first).all_fields.include? source
                                           TSV.index(database.identifiers, :target => source, :persist => true)
                                         else
                                           {}
                                         end
                                       else
                                         if TSV.parse_header(Organism.identifiers(namespace)).all_fields.include? source
                                           Organism.identifiers(namespace).index(:target => source, :persist => true)
                                         else
                                           {}
                                         end
                                       end
                                     end

    @identifiers[name]['source'][entity]
  end

  def identify_target(name, entity)
    database = get_database(name, :persist => true)
    target = target(name)

    @identifiers[name] ||= {}
    @identifiers[name]['target'] ||= begin
                                       if database.identifier_files.any?
                                         if TSV.parse_header(database.identifier_files.first).all_fields.include? target
                                           TSV.index(database.identifiers, :target => target, :persist => true)
                                         else
                                           {}
                                         end
                                       else
                                         if TSV.parse_header(Organism.identifiers(namespace)).all_fields.include? target
                                           Organism.identifiers(namespace).index(:target => target, :persist => true)
                                         else
                                          database.index(:target => database.fields.first, :fields => [database.fields.first], :persist => true)
                                         end
                                       end
                                     end
    @identifiers[name]['target'][entity]
  end

  def identify(name, entity)
    identify_source(name, entity) || identify_target(name, entity)
  end

  #{{{ Query
  
  def setup(name, matches, reverse = false)
    AssociationItem.setup matches, self, name, reverse
  end

  def all(name, options={})
    repo = get_index name, options
    setup name, repo.keys
  end

  def children(name, entity)
    repo = get_index name
    setup(name, repo.match(entity))
  end

  def parents(name, entity)
    repo = get_index name
    setup(name, repo.reverse.match(entity))
  end

  def neighbours(name, entity)
    if undirected(name)
      IndiferentHash.setup({:children => children(name, entity)})
    else
      IndiferentHash.setup({:parents => parents(name, entity), :children => children(name, entity)})
    end
  end

  def subset(name, entities)
    case entities
    when AnnotatedArray
      format = entities.format if entities.respond_to? :format 
      format ||= entities.base_entity.to_s
      {format => entities.clean_annotations}
    when Hash
    else
      raise "Entities are not a Hash or an AnnotatedArray: #{Misc.fingerprint entities}"
    end
    repo = get_index name
    begin
      setup(name, repo.subset_entities(entities))
    rescue 
      setup(name, repo.reverse.subset_entities(entities), true)
    end
  end

  def translate(entities, type)
    if format = @format[type] and format != entities.format
      entities.to format
    else
      entities
    end
  end
end
