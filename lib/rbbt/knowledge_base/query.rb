require 'rbbt/knowledge_base/registry'

class KnowledgeBase

  def _subset(name, source = :all, target = :all, options = {})
    repo = get_index name, options

    repo.subset(source, target)
  end

  def subset(name, entities, options = {}, &block)
    entities, options = options, entities if entities.nil? and Hash === options
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

    source, target = select_entities(name, entities, options)

    return [] if source.nil? or target.nil?
    return [] if Array === target and target.empty?
    return [] if Array === source and source.empty?

    matches = _subset name, source, target, options

    setup(name, matches)

    matches = matches.select(&block) if block_given? 

    matches
  end

  def all(name, options={})
    repo = get_index name, options
    setup name, repo.keys
  end

  def _children(name, entity)
    repo = get_index name
    repo.match(entity)
  end

  def children(name, entity)
    entity = identify_source(name, entity)
    setup(name, _children(name, entity))
  end

  def _parents(name, entity)
    repo = get_index name
    repo.reverse.match(entity)
  end

  def parents(name, entity)
    entity = identify_target(name, entity)
    matches = _parents(name, entity)
    matches.each{|m| m.replace(m.partition("~").reverse*"") } unless undirected(name)
    setup(name, matches, true)
  end

  def _neighbours(name, entity)
    if undirected(name) and source(name) == target(name)
      {:children => _children(name, entity)}
    else
      {:parents => _parents(name, entity), :children => _children(name, entity)}
    end
  end

  def neighbours(name, entity)
    hash = _neighbours(name, entity)
    IndiferentHash.setup(hash)
    setup(name, hash[:children]) if hash[:children] 
    setup(name, hash[:parents], true) if hash[:parents]
    hash
  end

end

