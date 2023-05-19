require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/knowledge_base/registry'


class TestKnowledgeBaseRegistry < Test::Unit::TestCase

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

    @knowledge_base = KnowledgeBase.new '/tmp/kb.foo2'

    @knowledge_base.register :effects, @effect_tsv, @effect_options.dup

    @knowledge_base.register :pina, datafile_test('pina'), :source => "UniProt/SwissProt Accession", :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession", :undirected => true
  end

  def test_database
    assert_equal "Associated Gene Name", @knowledge_base.get_database(:effects, :source_format => "Associated Gene Name").key_field
  end

  def test_index
    assert @knowledge_base.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => false).include? "MDM2~ENSG00000141510"
  end

  def test_index_persist
    assert @knowledge_base.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => true).include? "MDM2~ENSG00000141510"
  end

  def test_index_flat
    require 'rbbt/sources/tfacts'
    file = TFactS.regulators
    @knowledge_base.register :tfacts, file,  :type => :flat, :source => "Transcription Factor Associated Gene Name=~Associated Gene Name", :merge => true
    assert @knowledge_base.subset(:tfacts, :source => ["TP53"], :target => :all).length > 10
  end

  def test_pina
    index = @knowledge_base.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name")
    assert index["TP53~ARID1A"]
    assert index["ARID1A~TP53"]
    assert_equal index["ARID1A~TP53"], index["TP53~ARID1A"]

    index = @knowledge_base.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name", :undirected => false)
    count = 0
    index.through do |k,values|
      split_values = values.collect{|v| v.split ";;" }
      count += 1 if Misc.zip_fields(split_values).uniq != Misc.zip_fields(split_values)
    end

    index = @knowledge_base.get_index(:pina, :persist => false, :source_format => "Associated Gene Name", :target_format => "Associated Gene Name", :undirected => true)
    count2 = 0
    index.through do |k,values|
      split_values = values.collect{|v| v.split ";;" }
      count2 += 1 if Misc.zip_fields(split_values).uniq != Misc.zip_fields(split_values)
    end

  end

  def test_pina2
    @knowledge_base.entity_options["Gene"] = {:organism => "Mmu"}
    index = @knowledge_base.get_index(:pina, :persist => true, :source_format => "Ensembl Gene ID", :target_format => "Ensembl Gene ID", :undirected => true)
    assert_equal "Mmu", index.entity_options["Gene"][:organism]
  end
end

