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

  def with_kb
    TmpFile.with_file(EFFECT) do |file|
      tsv = TSV.open file
      kb = KnowledgeBase.new '/tmp/kb.foo2', "Hsa"
      kb.format = {"Gene" => "Ensembl Gene ID"}
      kb.entity_options["Gene"] = {:organism => "Mmu", :test_option => "TEST"}

      kb.register :effects, tsv, EFFECT_OPTIONS.dup
      kb
    end
  end


  def test_syndicate_entity_options
    with_kb do |orig|
      Gene.add_identifiers datafile_test('identifiers')
      kb = KnowledgeBase.new "/tmp/kb.foo3", "Hsa"
      kb.format = {"Gene" => "Associated Gene Name"} 
      kb.syndicate :orig, orig
      assert_equal "Mmu", orig.entity_options_for("Gene", "effects")[:organism]
      assert_equal "Mmu", kb.get_index("effects@orig").entity_options["Gene"][:organism]
      assert_equal "Mmu", kb.get_database("effects@orig").entity_options["Gene"][:organism]
      assert_equal "Mmu", kb.entity_options_for("Gene", "effects@orig")[:organism]
      assert_equal "Mmu", kb.children("effects@orig", "TP53").source_entity.organism
    end
  end
end

