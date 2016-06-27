
require 'rbbt/association'
require 'rbbt/association/item'
require 'rbbt/knowledge_base/entity'
require 'rbbt/knowledge_base/query'
require 'rbbt/knowledge_base/syndicate'

class KnowledgeBase

  attr_accessor :namespace, :dir, :databases, :indices, :registry, :format, :entity_options
  def initialize(dir, namespace = nil)
    @dir = Path.setup(dir.dup)

    @namespace = namespace
    @format = IndiferentHash.setup({})

    @registry ||= IndiferentHash.setup({})
    @entity_options = IndiferentHash.setup({})

    @indices = IndiferentHash.setup({})
    @databases = IndiferentHash.setup({})
    @identifiers = IndiferentHash.setup({})
    @fields = {}
    @descriptions = {}
    @databases = {}
  end

  def self.load(dir)
    KnowledgeBase.new dir
  end

  def setup(name, matches, reverse = false)
    AssociationItem.setup matches, self, name, reverse
  end
end
