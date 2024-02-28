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

  def with_kb(&block)
    keyword_test :organism do
      require 'rbbt/sources/organism'
      organism = Organism.default_code("Hsa")
      TmpFile.with_file do |tmpdir|
        kb = KnowledgeBase.new tmpdir, "Hsa"
        kb.format = {"Gene" => "Associated Gene Name"}

        kb.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

        yield kb
      end
    end
  end

  def test_entity_options
    with_kb do |kb|
      kb.entity_options = {"Gene" => {:organism => "Mmu"}}
      assert_include kb.children(:effects, "TP53").target_entity.to("Associated Gene Name"), "GLI1"
      assert_equal "Mmu", kb.children(:effects, "TP53").target_entity.organism
    end
  end

  def test_source_type
    with_kb do |kb|
      assert_match "Gene", kb.source_type(:effects).to_s
    end
  end

end

