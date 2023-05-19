require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/query'

class TestKnowledgeBaseQuery < Test::Unit::TestCase

  setup do
    @effect =StringIO.new <<-END
#: :sep=" "#:type=:double
#SG TG Effect
MDM2 TP53 inhibition
TP53 NFKB1|GLI1 activation|activation true|true
    END

    @effect_options = {
      :source => "SG=~Associated Gene Name=>Ensembl Gene ID",
      :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
      :persist => false,
      :identifiers => datafile_test('identifiers'),
      :undirected => true,
      :namespace => "Hsa"
    }

    @effect_tsv = TSV.open @effect, @effect_options.dup

    @knowledge_base = KnowledgeBase.new Rbbt.tmp.test.kb_foo2, "Hsa"
    @knowledge_base.format = {"Gene" => "Ensembl Gene ID"}

    @knowledge_base.register :effects, @effect_tsv, @effect_options.dup
  end

  def test_subset_all_persist
    Misc.benchmark(1000) do
      assert_equal 6, @knowledge_base.subset(:effects, :all).length

      assert_equal 4, @knowledge_base.subset(:effects, :all).target_entity.uniq.length
      assert_equal %w(Effect), @knowledge_base.subset(:effects, :all).info.first.keys
    end
  end

  def _test_subset_all_persist_format
    assert @knowledge_base.subset(:effects, :all).target_entity.reject{|e| e =~ /^ENS/}.empty?
  end

end

