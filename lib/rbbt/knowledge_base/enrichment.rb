require 'rbbt/knowledge_base/registry'
class KnowledgeBase
  def enrichment(name, entities, options = {})
    require 'rbbt/statistics/hypergeometric'
    database = get_database(name, options)
    entities = identify_source name, entities
    database.enrichment entities, database.fields.first, :persist => false
  end
end
