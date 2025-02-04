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

  EFFECT =<<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
  END

  EFFECT_OPTIONS = {
    :source => "SG=~SG (Associated Gene Name)",
    :target => "TG=~TG (Associated Gene Name)=>Ensembl Gene ID",
    :undirected => true,
    :persist => true,
    :namespace => "Hsa"
  }
  Gene.add_identifiers datafile_test('identifiers')

  #EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup 

  def with_kb(&block)
    keyword_test :organism do
      require 'rbbt/sources/organism'
      organism = Organism.default_code("Hsa")
      TmpFile.with_file do |tmpdir|
        TmpFile.with_file(EFFECT) do |file|
          tsv = TSV.open(file)
          kb = KnowledgeBase.new tmpdir, "Hsa"
          kb.format = {"Gene" => "Associated Gene Name"}

          kb.register :effects, tsv, EFFECT_OPTIONS.dup

          yield kb
        end
      end
    end
  end

  def test_identify_source
    with_kb do |kb|
      assert_equal "TP53", kb.identify_source(:effects, "ENSG00000141510")
    end
  end

  def test_entity_options
    with_kb do |kb|
      kb.entity_options = {"Gene" => {:namespace => "Mmu"}}
      assert_not_equal "", kb.children(:effects, "TP53").target_entity.first
      assert_include kb.children(:effects, "TP53").target_entity.to("Associated Gene Name"), "GLI1"
      assert_equal "Mmu", kb.children(:effects, "TP53").target_entity.namespace
    end
  end

  def test_source_type
    with_kb do |kb|
      assert_match "Gene", kb.source_type(:effects).to_s
    end
  end

end

