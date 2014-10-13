
require 'rbbt/association'
require 'rbbt/association/item'
require 'rbbt/knowledge_base/entity'
require 'rbbt/knowledge_base/query'
require 'rbbt/knowledge_base/syndicate'

class KnowledgeBase

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

  def setup(name, matches, reverse = false)
    AssociationItem.setup matches, self, name, reverse
  end
end
