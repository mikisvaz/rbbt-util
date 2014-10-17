require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/query'

module Gene
  extend Entity
end
class TestKnowledgeBaseSyndicate < Test::Unit::TestCase

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

  KNOWLEDGE_BASE = KnowledgeBase.new '/tmp/kb.foo2', "Hsa"
  KNOWLEDGE_BASE.format = {"Gene" => "Ensembl Gene ID"}
  KNOWLEDGE_BASE.entity_options["Gene"] = {:organism => "Mmu", :test_option => "TEST"}

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

  def test_syndicate_entity_options
    Gene.add_identifiers datafile_test('identifiers')
    kb = KnowledgeBase.new "/tmp/kb.foo3", "Hsa"
    kb.format = {"Gene" => "Associated Gene Name"} 
    kb.syndicate :orig, KNOWLEDGE_BASE
    assert_equal "Mmu", KNOWLEDGE_BASE.entity_options_for("Gene", "effects")[:organism]
    assert_equal "Mmu", kb.get_index("effects@orig").entity_options["Gene"][:organism]
    assert_equal "Mmu", kb.get_database("effects@orig").entity_options["Gene"][:organism]
    assert_equal "Mmu", kb.entity_options_for("Gene", "effects@orig")[:organism]
    assert_equal "Mmu", kb.children("effects@orig", "TP53").source_entity.organism
  end
end

