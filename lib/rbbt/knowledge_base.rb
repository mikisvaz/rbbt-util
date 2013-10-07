require 'rbbt/association'
require 'rbbt/entity'

class KnowledgeBase
  class << self
    attr_accessor :knowledge_base_dir
    
    def knowledge_base_dir
      @knowledge_base_dir ||= Rbbt.var.knowledge_base
    end
  end

  attr_accessor :info, :dir, :repos
  def initialize(dir, namespace = nil)
    @dir = Path.setup dir
    @namespace = namespace
    @info = {}
    @repos = {}
  end

  def index(name, file, options = {}, persist_options = {})
    options = Misc.add_defaults options, {:namespace => @namespace}
    @repos[name] = Association.index(file, options, persist_options)
  end

  def add_database(name, source_type, target_type, *fields)
    options = fields.pop if Hash === fields.last
    options ||= {}
    undirected = Misc.process_options options, :undirected 

    undirected = nil unless undirected 

    repo_file = dir[name].find
    @repos[name] = Association.index(nil, {:namespace => namespace, :key_field => [source_type, target_type, undirected].compact * "~", :fields => fields}.merge(options), :file => repo_file)
  end

  def add(name, source, target, *rest)
    code = [source, target] * "~"
    repo = @repos[name]
    repo[code] = rest
  end

  def write(name)
    repo = @repos[name]
    repo.write_and_close do
      yield
    end
  end

  #def self.global
  #  @kb ||= KnowledgeBase.new knowledge_base_dir.global.find do 
  #    Association.databases.each do |database, info|
  #      file, options = info
  #      options ||= {}
  #      options[:namespace] = "Hsa/jan2013"
  #      options[:source_type] = "Ensembl Gene ID"
  #      options[:target_type] = "Ensembl Gene ID"

  
  #      register database, file, options
  #    end
  #  end
  #end

  #attr_accessor :dir, :info, :entity_types
  #def initialize(dir, &block)
  #  @dir = dir
  #  @info = {"All" => {}}

  #  self.instance_eval &block if block_given?  and not File.exists? dir
  #end

  #def all_databases
  #  Dir.glob(dir + '/*').collect{|f| File.basename f }.reject{|f| f =~ /\.reverse$/ }
  #end

  #def database_info
  #  info = {}
  #  all_databases.each do |name|
  #    repo = get_repo name
  #    target, source, undirected = database_types name
  #    fields = repo.fields
  #    info[name] = {:target => target, :source => source, :undirected => undirected, :info => fields}
  #  end
  #end

  #def association_sources
  #  all_databases
  #end

  #def repo_file(name)
  #  raise "No repo specified" if name.nil? or name.empty?
  #  File.join(dir, name.to_s)
  #end

  #def get_repo(name)
  #  @repos ||= {}
  #  @repos[name] ||= begin
  #                    file = repo_file name
  #                    File.exists?(file) ?
  #                      Persist.open_tokyocabinet(file, false, nil, TokyoCabinet::BDB) :
  #                      nil
  #                  end
  #end

  #def database_types(name)
  #  repo = get_repo(name)
  #  return nil if repo.nil?
  #  source, target = repo.key_field.split "~"
  #  source_type = Entity.formats[source] || source
  #  target_type = Entity.formats[target] || target
  #  [source_type, target_type]
  #end

  #def init_entity_registry
  #  @sources = {}
  #  @targets = {}
  #  all_databases.each do |repo|
  #    source_type, target_type = database_types repo
  #    @sources[source_type] ||= []
  #    @sources[source_type] << repo
  #    @targets[target_type] ||= []
  #    @targets[target_type] << repo
  #  end
  #end

  #def sources
  #  init_entity_registry unless @sources
  #  @sources
  #end

  #def targets
  #  init_entity_registry unless @targets
  #  @targets
  #end

  #def entity_types
  #  (sources.keys + targets.keys).uniq
  #end

  #def register(database, file, options)
  #  persistence = repo_file database
  #  Association.index(file, options, :persist => true, :file => persistence)
  #end

  #def connections(database, entities)
  #  repo = get_repo(database)
  #  return [] if repo.nil?
  #  Association.connections(repo, entities)
  #end

  #def neighbours(name, entities)
  #  repo = get_repo(name)
  #  return nil if repo.nil?

  #  source_type, target_type = database_types name

  #  case
  #  when ((list = entities[source_type.to_s]) and list.any?)
  #    {:type => target_type, :entities => Association.neighbours(repo, list)}
  #  when ((list = entities[target_type.to_s]) and list.any?)
  #    {:type => source_type, :entities => Association.neighbours(repo, list)}
  #  else
  #    nil
  #  end
  #end

  #def query(term)
  #  query_entities = {term.base_entity.to_s => [term]}
  #  all_databases.inject({}){|acc,database|
  #    if neigh = neighbours(database, query_entities)
  #      acc[database] = neigh
  #      acc
  #    else
  #      acc
  #    end
  #  }
  #end

  #def add_database(name, source_type, target_type, undirected = false)
  #end
end
