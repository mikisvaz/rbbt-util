require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt-util'
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/entity'

module Gene
  extend Entity
end

class TestKnowledgeEnity < Test::Unit::TestCase

  EFFECT =StringIO.new <<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
  END

  EFFECT_OPTIONS = {
    :source => "SG=~Associated Gene Name",
    :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
    :undirected => true,
    :persist => true,
    :namespace => "Hsa"
  }
  Gene.add_identifiers datafile_test('identifiers')

  EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup 

  KNOWLEDGE_BASE = KnowledgeBase.new '/tmp/kb.foo3', "Hsa"
  KNOWLEDGE_BASE.format = {"Gene" => "Associated Gene Name"}

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

  def test_entity_options
    KNOWLEDGE_BASE.entity_options = {"Gene" => {:organism => "Mmu"}}
    assert_equal "Mmu", KNOWLEDGE_BASE.children(:effects, "TP53").target_entity.organism
  end
end

