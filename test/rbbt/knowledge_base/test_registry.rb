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

  def with_kb(&block)
    keyword_test :organism do
      require 'rbbt/sources/organism'
      organism = Organism.default_code("Hsa")
      TmpFile.with_file do |tmpdir|
        kb = KnowledgeBase.new tmpdir
        kb.namespace = organism
        kb.format = {"Gene" => "Associated Gene Name"}

        kb.register :effects, EFFECT_TSV, EFFECT_OPTIONS
        kb.register :pina, datafile_test('pina'), 
          :source => "UniProt/SwissProt Accession", 
          :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession", 
          :undirected => true

        kb.register :gene_ages, datadir_test.gene_ages, :source => "=>Associated Gene Name"

        kb.register :CollecTRI, datadir_test.CollecTRI, 
          :source => "Transcription Factor=~Associated Gene Name", 
          :target => "Target Gene=~Associated Gene Name",
          :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"]

        yield kb
      end
    end
  end

  def test_database
    with_kb do |kb|
      assert_equal "Associated Gene Name", kb.get_database(:effects, :source_format => "Associated Gene Name").key_field
    end
  end

  def test_index
    with_kb do |kb|
      assert kb.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => false).include? "MDM2~ENSG00000141510"
    end
  end

  def test_index_persist
    with_kb do |kb|
      assert kb.get_index(:effects, :source_format => "Associated Gene Name", :target_format => "Ensembl Gene ID", :persist => true).include? "MDM2~ENSG00000141510"
    end
  end

end

