$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/sources/pina'

require 'rbbt/workflow'
Workflow.require_workflow "Genomics"
require 'knowledge_base'

class TestKnowledgeBase < Test::Unit::TestCase
  def _setup
    KnowledgeBase.knowledge_base_dir = Rbbt.tmp.knowledge_base_test.find
    @kb = KnowledgeBase.global
  end

  def _test_register
    TmpFile.with_file do |dir|
      kb = KnowledgeBase.new dir

      kb.register :pina, Pina.protein_protein, :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession"
      assert_equal [Gene], kb.entity_types
      assert kb.all_databases.include? "pina"
    end
  end

  def _test_global
    assert @kb.all_databases.include? "pina"
  end

  def _test_query
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013")
    Misc.profile do
    Misc.benchmark(10) do
      @kb.query(tp53.ensembl)
    end
    end
    assert_include @kb.query(tp53.ensembl), 'go'
  end

  def _test_benchmark
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    kb = KnowledgeBase.new Rbbt.tmp.test.kb
    kb.info[:namespace] = "Hsa/jan2013"

      require 'rbbt/sources/COSMIC'
      require 'rbbt/entity/genomic_mutation'
      mutations = tp53.COSMIC_mutations
      Misc.benchmark(10) do 
        name = "mutations"
        kb.add_database name, "Ensembl Gene ID", "Genomic Mutation", "Change"
        kb.write name do
          mutations.each do |gm|
            kb.add name, tp53, gm, gm.base
          end
        end
      end
  end

  def test_benchmark2
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    kb = KnowledgeBase.new Rbbt.tmp.test.kb2, "Hsa/jan2013"
    kb.index('g2t', Organism.gene_transcripts("Hsa/jan2013"), :target => "Ensembl Transcript ID")
    l = nil
    Misc.benchmark(80000) do
      kb.repos['g2t'].match(tp53)
    end
    puts l
    Misc.benchmark(1000) do
     l = tp53.transcripts.length
    end
    puts l
  end

end

