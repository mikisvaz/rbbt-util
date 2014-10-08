require 'rbbt/entity'
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
end

