require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/workflow'
require 'rbbt/entity'

require 'rbbt/association'
require 'rbbt/knowledge_base'


class TestKnowledgeBase < Test::Unit::TestCase

  EFFECT =StringIO.new <<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
  END

  EFFECT_OPTIONS = {
    :source => "SG=~Associated Gene Name",
    :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
    :persist => false,
    :identifiers => datafile_test('identifiers'),
    :undirected => true,
    :namespace => "Hsa"
  }

  EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup 

  KNOWLEDGE_BASE = KnowledgeBase.new '/tmp/kb.foo'

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup
end

