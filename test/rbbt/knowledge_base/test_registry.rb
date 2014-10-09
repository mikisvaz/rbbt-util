require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/registry'


class TestKnowledgeBaseRegistry < Test::Unit::TestCase

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

  KNOWLEDGE_BASE = KnowledgeBase.new '/tmp/kb.foo'

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

  def test_database
    assert_equal "Associated Gene Name", KNOWLEDGE_BASE.get_database(:effects).key_field
  end

  def test_index
    assert KNOWLEDGE_BASE.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => false).include? "MDM2~ENSG00000141510"
  end

  def test_index_flat
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    KNOWLEDGE_BASE.register :tfacts, file,  :type => :flat, :source => "Transcription Factor Associated Gene Name=~Associated Gene Name", :merge => true
    assert KNOWLEDGE_BASE.subset(:tfacts, :source => ["TP53"], :target => :all).length > 10
  end
end

