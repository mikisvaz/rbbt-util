require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/query'

module Gene
  extend Entity
end
class TestKnowledgeBaseSyndicate < Test::Unit::TestCase

  setup do
    @effect =StringIO.new <<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
    END

    @effect_options = {
      :source => "SG=~Associated Gene Name",
      :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
      :persist => false,
      :identifiers => datafile_test('identifiers'),
      :undirected => true,
      :namespace => "Hsa"
    }

    @effect_tsv = TSV.open @effect, @effect_options.dup 

    @knowledge_base = KnowledgeBase.new '/tmp/kb.foo2', "Hsa"
    @knowledge_base.format = {"Gene" => "Ensembl Gene ID"}
    @knowledge_base.entity_options["Gene"] = {:organism => "Mmu", :test_option => "TEST"}

    @knowledge_base.register :effects, @effect_tsv, @effect_options.dup
  end

  def test_syndicate_entity_options
    Gene.add_identifiers datafile_test('identifiers')
    kb = KnowledgeBase.new "/tmp/kb.foo3", "Hsa"
    kb.format = {"Gene" => "Associated Gene Name"} 
    kb.syndicate :orig, @knowledge_base
    assert_equal "Mmu", @knowledge_base.entity_options_for("Gene", "effects")[:organism]
    assert_equal "Mmu", kb.get_index("effects@orig").entity_options["Gene"][:organism]
    assert_equal "Mmu", kb.get_database("effects@orig").entity_options["Gene"][:organism]
    assert_equal "Mmu", kb.entity_options_for("Gene", "effects@orig")[:organism]
    assert_equal "Mmu", kb.children("effects@orig", "TP53").source_entity.organism
  end
end

