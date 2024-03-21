require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/query'

class TestKnowledgeBaseQuery < Test::Unit::TestCase

  EFFECT =<<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
  END

  EFFECT_OPTIONS = {
    :source => "SG=~Associated Gene Name=>Ensembl Gene ID",
    :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
    :persist => false,
    :identifiers => datafile_test('identifiers'),
    :undirected => true,
    :namespace => "Hsa"
  }

  #EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup

  def knowledge_base
    @kb ||= begin 
              TmpFile.with_file(EFFECT) do |f|
                kb = KnowledgeBase.new Rbbt.tmp.test.kb_foo2, "Hsa"
                kb.format = {"Gene" => "Ensembl Gene ID"}

                kb.register :effects, TSV.open(f), EFFECT_OPTIONS
                kb
              end
           end
  end

  def test_effects
    knowledge_base.get_database :effects
  end

  def __test_benchmark
    assert_equal 6, knowledge_base.subset(:effects, :all).length
    Misc.benchmark(1000) do
      assert_equal 6, knowledge_base.subset(:effects, :all).length

      assert_equal 4, knowledge_base.subset(:effects, :all).target_entity.uniq.length
      assert_equal %w(Effect), knowledge_base.subset(:effects, :all).info.first.keys
    end
  end

  def test_subset_all_persist_format
    assert knowledge_base.subset(:effects, :all).target_entity.reject{|e| e =~ /^ENS/}.empty?
  end

end

