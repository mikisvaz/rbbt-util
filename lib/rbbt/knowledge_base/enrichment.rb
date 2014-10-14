require 'rbbt/knowledge_base/registry'
require 'rbbt/statistics/hypergeometric'
class KnowledgeBase
  def enrichment(name, entities, options = {})
    database = get_database(name, options)
    entities = identify_source name, entities
    database.enrichment entities, database.fields.first, :persist => false
  end
end
