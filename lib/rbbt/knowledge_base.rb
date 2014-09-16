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


  def setup(name, matches, reverse = false)
    AssociationItem.setup matches, self, name, reverse
  end

  attr_accessor :namespace, :dir, :indices, :registry, :format, :databases, :entity_options
  def initialize(dir, namespace = nil)
    @dir = Path.setup(dir.dup).find

    @namespace = namespace
    @format = IndiferentHash.setup({})

    @registry ||= IndiferentHash.setup({})
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

  def syndicate(name, kb)
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
 
  #def get_database(name, options = {})
  #  @databases[Misc.fingerprint([name, options])] ||= \
  #    begin 
  #      Persist.memory("Database:" <<[name, self.dir] * "@") do
  #        options = Misc.add_defaults options, :persist_dir => dir.databases
  #        persist_options = Misc.pull_keys options, :persist

  #        file, registered_options = registry[name]
  #        options = open_options.merge(registered_options || {}).merge(options)
  #        raise "Repo #{ name } not found and not registered" if file.nil?

  #        Log.low "Opening database #{ name } from #{ Misc.fingerprint file }. #{options}"
  #        Association.open(file, options, persist_options).
  #          tap{|tsv| tsv.namespace = self.namespace}
  #      end
  #    end
  #end


  #def get_index(name, options = {})
  #  @indices[Misc.fingerprint([name, options])] ||= \
  #    begin 
  #      Persist.memory("Index:" <<[name, self.dir] * "@") do
  #        options = Misc.add_defaults options, :persist_dir => dir.indices
  #        persist_options = Misc.pull_keys options, :persist

  #        file, registered_options = registry[name]
  #        options = open_options.merge(registered_options || {}).merge(options)
  #        raise "Repo #{ name } not found and not registered" if file.nil?

  #        Log.low "Opening index #{ name } from #{ Misc.fingerprint file }. #{options}"
  #        Association.index(file, options, persist_options).
  #          tap{|tsv| tsv.namespace = self.namespace}
  #      end
  #    end
  #end

  def get_database(name, options = {})
    key = name.to_s + "_" + Misc.digest(Misc.fingerprint([name,options,format,namespace]))
    @databases[key] ||= 
      begin 
        Persist.memory("Database:" << [key, dir] * "@") do
          persist_file = dir.databases[key]
          file, registered_options = registry[name]

          options = Misc.add_defaults options, :persist_file => persist_file, :namespace => namespace, :format => format
          options = Misc.add_defaults options, registered_options if registered_options

          persist_options = Misc.pull_keys options, :persist

          database = if persist_file.exists?
                    Log.low "Re-opening database #{ name } from #{ Misc.fingerprint persist_file }. #{options}"
                    Association.open(file, options, persist_options)
                  else
                    raise "Repo #{ name } not found and not registered" if file.nil?
                    Log.low "Opening database #{ name } from #{ Misc.fingerprint file }. #{options}"
                    Association.open(file, options, persist_options)
                  end

          database.namespace = self.namespace

          database
        end
      end
  end

  def get_index(name, options = {})
    key = name.to_s + "_" + Misc.digest(Misc.fingerprint([name,options]))
    @indices[key] ||= 
      begin 
        Persist.memory("Index:" << [key, dir] * "@") do
          persist_file = dir.indices[key]
          file, registered_options = registry[name]

          options = Misc.add_defaults options, :persist_file => persist_file, :namespace => namespace, :format => format
          options = Misc.add_defaults options, registered_options if registered_options and registered_options.any?

          persist_options = Misc.pull_keys options, :persist

          index = if persist_file.exists?
                    Log.low "Re-opening index #{ name } from #{ Misc.fingerprint persist_file }. #{options}"
                    Association.index(nil, options, persist_options)
                  else
                    options = Misc.add_defaults options, registered_options if registered_options
                    raise "Repo #{ name } not found and not registered" if file.nil?
                    Log.low "Opening index #{ name } from #{ Misc.fingerprint file }. #{options}"
                    Association.index(file, options, persist_options)
                  end

          index.namespace = self.namespace

          index
        end
      end
  end


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
    format = @format[type] || type
    Misc.prepare_entity(entities, format, entity_options_for(type, database))
  end

  #{{{ Identify
  

  def database_identify_index(database, target)
    if database.identifier_files.any?
      id_file =  database.identifier_files.first
      identifier_fields = TSV.parse_header(id_file).all_fields
      if identifier_fields.include? target
        TSV.index(database.identifiers, :target => target, :persist => true, :order => true)
      else
        {}
      end
    else
      if TSV.parse_header(Organism.identifiers(namespace)).all_fields.include? target
        Organism.identifiers(namespace).index(:target => target, :persist => true, :order => true)
      else
        {}
      end
    end
  end

  def identify_source(name, entity)
    database = get_database(name, :persist => true)
    return entity if Symbol === entity or (String === entity and database.include? entity)
    source = source(name)
    @identifiers[name] ||= {}
    @identifiers[name]['source'] ||= begin
                                       database_identify_index(database, source)
                                     end

    if Array === entity
      @identifiers[name]['source'].chunked_values_at(entity).zip(entity).collect{|p|
        p.compact.first
      }
    else
      @identifiers[name]['source'][entity]
    end
  end

  def identify_target(name, entity)
    database = get_database(name, :persist => true)
    return entity if Symbol === entity or (String === entity and database.values.collect{|v| v.first}.compact.flatten.include?(entity))
    target = target(name)

    @identifiers[name] ||= {}
    @identifiers[name]['target'] ||= begin
                                       database_identify_index(database, target)
                                     end
    if Array === entity
      @identifiers[name]['target'].chunked_values_at(entity).zip(entity).collect{|p|
        p.compact.first
      }
    else
      @identifiers[name]['target'][entity] 
    end
  end

  def identify(name, entity)
    identify_source(name, entity) || identify_target(name, entity)
  end

  def normalize(entity)
    source_matches = all_databases.collect{|d|
      identify_source(d, entity)
    }.flatten.compact.uniq
    return entity if source_matches.include? entity

    target_matches = all_databases.collect{|d|
      identify_target(d, entity)
    }.flatten.compact.uniq
    return entity if target_matches.include? entity

    (source_matches + target_matches).first
  end

  #{{{ Query

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
    if undirected(name) and source(name) == target(name)
      IndiferentHash.setup({:children => children(name, entity)})
    else
      IndiferentHash.setup({:parents => parents(name, entity), :children => children(name, entity)})
    end
  end

  def subset(name, entities)
    entities = case entities
               when :all
                 {:target => :all, :source => :all}
               when AnnotatedArray
                 format = entities.format if entities.respond_to? :format 
                 format ||= entities.base_entity.to_s
                 {format => entities.clean_annotations}
               when Hash
                 entities
               else
                 raise "Entities are not a Hash or an AnnotatedArray: #{Misc.fingerprint entities}"
               end

    repo = get_index name

    begin
      s = repo.subset_entities(entities)
      setup(name, s)
    rescue Exception
      target = entities[:target]
      source = entities[:source]
      if target or source
        entities[:target] = source 
        entities[:source] = target
      end
      setup(name, repo.reverse.subset_entities(entities), true)
    end
  end

  def translate(entities, type)
    if format = @format[type] and (entities.respond_to? :format and format != entities.format)
      entities.to format
    else
      entities
    end
  end

  def pair_matches(source, target, undirected = nil)
    all_databases.inject([]){|acc,database|
      match = [source, target] * "~"
      index = get_index(database)

      if index.include? match 
        acc << setup(database, match) 

      elsif undirected or undirected(database) 
        inv = [target, source] * "~"
        if index.include? inv 
          setup(database, inv)
          acc <<  inv 
        end
      end

      acc
    }
  end
end
