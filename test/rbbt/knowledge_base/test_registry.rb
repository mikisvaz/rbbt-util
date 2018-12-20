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

  KNOWLEDGE_BASE = KnowledgeBase.new '/tmp/kb.foo2'

  KNOWLEDGE_BASE.register :effects, EFFECT_TSV, EFFECT_OPTIONS.dup

  KNOWLEDGE_BASE.register :pina, datafile_test('pina'), :source => "UniProt/SwissProt Accession", :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession", :undirected => true

  def test_database
    assert_equal "Associated Gene Name", KNOWLEDGE_BASE.get_database(:effects, :source_format => "Associated Gene Name").key_field
  end

  def test_index
    assert KNOWLEDGE_BASE.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => false).include? "MDM2~ENSG00000141510"
  end

  def test_index_persist
    assert KNOWLEDGE_BASE.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => true).include? "MDM2~ENSG00000141510"
  end

  def test_index_flat
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    KNOWLEDGE_BASE.register :tfacts, file,  :type => :flat, :source => "Transcription Factor Associated Gene Name=~Associated Gene Name", :merge => true
    assert KNOWLEDGE_BASE.subset(:tfacts, :source => ["TP53"], :target => :all).length > 10
  end

  def test_pina
    index = KNOWLEDGE_BASE.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name")
    assert index["TP53~ARID1A"]
    assert index["ARID1A~TP53"]
    assert_equal index["ARID1A~TP53"], index["TP53~ARID1A"]

    index = KNOWLEDGE_BASE.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name", :undirected => false)
    count = 0
    index.through do |k,values|
      split_values = values.collect{|v| v.split ";;" }
      count += 1 if Misc.zip_fields(split_values).uniq != Misc.zip_fields(split_values)
    end

    index = KNOWLEDGE_BASE.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name", :undirected => true)
    count2 = 0
    index.through do |k,values|
      split_values = values.collect{|v| v.split ";;" }
      count2 += 1 if Misc.zip_fields(split_values).uniq != Misc.zip_fields(split_values)
    end

  end

  def test_pina2
    KNOWLEDGE_BASE.entity_options["Gene"] = {:organism => "Mmu"}
    index = KNOWLEDGE_BASE.get_index(:pina, :persist => true, :source_format => "Ensembl Gene ID", :target_format => "Ensembl Gene ID", :undirected => true)
    assert_equal "Mmu", index.entity_options["Gene"][:organism]
  end
end

