require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/query'

require 'rbbt/workflow'
Workflow.require_workflow "Genomics"
require 'rbbt/knowledge_base/Genomics'

class TestKnowledgeBaseQuery < Test::Unit::TestCase

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

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

  def test_subset_all_persist
    Misc.benchmark(1000) do
      assert_equal 6, KNOWLEDGE_BASE.subset(:effects, :all).length
      assert_equal 4, KNOWLEDGE_BASE.subset(:effects, :all).target_entity.uniq.length
      assert_equal %w(Effect), KNOWLEDGE_BASE.subset(:effects, :all).info.first.keys 
    end
  end

  def test_subset_all_persist_format
    assert KNOWLEDGE_BASE.subset(:effects, :all).target_entity.reject{|e| e =~ /^ENS/}.empty?
  end

end

