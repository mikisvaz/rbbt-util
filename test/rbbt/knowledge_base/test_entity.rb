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
      :undirected => true,
      :persist => true,
      :namespace => "Hsa"
    }
    Gene.add_identifiers datafile_test('identifiers')

    @effect_tsv = TSV.open @effect, @effect_options.dup 

    @knowledge_base = KnowledgeBase.new '/tmp/kb.foo3', "Hsa"
    @knowledge_base.format = {"Gene" => "Associated Gene Name"}

    @knowledge_base.register :effects, @effect_tsv, @effect_options.dup
  end

  def test_entity_options
    @knowledge_base.entity_options = {"Gene" => {:organism => "Mmu"}}
    assert_equal "Mmu", @knowledge_base.children(:effects, "TP53").target_entity.organism
  end
end

