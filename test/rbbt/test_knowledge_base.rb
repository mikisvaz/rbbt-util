$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'test/unit'
require 'rbbt/knowledge_base'
require 'rbbt/sources/pina'

class TestKnowledgeBase < Test::Unit::TestCase
  def setup
    require 'rbbt/workflow'
    Workflow.require_workflow "Genomics"
    require 'genomics_kb'


    KnowledgeBase.knowledge_base_dir = Rbbt.tmp.knowledge_base_test.find
    @kb = Genomics.knowledge_base
  end

  def test_register
    TmpFile.with_file do |dir|
      kb = KnowledgeBase.new dir

      kb.register :pina, Pina.protein_protein, :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession"
      assert_equal [Gene], kb.entity_types
      assert kb.all_databases.include? :pina
    end
  end

  def test_format_Gene
    TmpFile.with_file do |dir|
      kb = KnowledgeBase.new dir, "Hsa/jan2013"
      kb.format["Gene"] = "Ensembl Gene ID"

      kb.register 'nature', NCI.nature_pathways, :merge => true, :target => "UniProt/SwissProt Accession", :key_field => 0

      assert kb.get_database('nature', :persist => false).slice("Ensembl Gene ID").values.flatten.uniq.length > 10
    end
  end

  def test_fields
    TmpFile.with_file do |dir|
      kb = KnowledgeBase.new dir, "Hsa/jan2013"
      kb.format["Gene"] = "Ensembl Gene ID"

      kb.register 'nature', NCI.nature_pathways, :merge => true, :fields => [2], :key_field => 0
      assert kb.get_database('nature', :persist => false).slice("Ensembl Gene ID").values.flatten.uniq.length > 10
    end
  end

  def test_global
    assert @kb.all_databases.include? "pina"
  end

  def test_benchmark
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    kb = KnowledgeBase.new Rbbt.tmp.test.kb2
    kb.namespace = "Hsa/jan2013"

      require 'rbbt/sources/COSMIC'
      require 'rbbt/entity/genomic_mutation'
      mutations = tp53.COSMIC_mutations
      Misc.benchmark(10) do 
        name = "mutations"
        kb.add_index name, "Ensembl Gene ID", "Genomic Mutation", "Change"
        kb.write name do
          mutations.each do |gm|
            kb.add name, tp53, gm, gm.base
          end
        end
      end
  end

  def test_items
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    kb = KnowledgeBase.new Rbbt.tmp.test.kb2, "Hsa/jan2013"
    kb.index('g2t', Organism.gene_transcripts("Hsa/jan2013"), :target => "Ensembl Transcript ID")
  end

  def test_benchmark2
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    kb = KnowledgeBase.new Rbbt.tmp.test.kb2, "Hsa/jan2013"
    kb.index('g2t', Organism.gene_transcripts("Hsa/jan2013"), :target => "Ensembl Transcript ID")
    l = nil
    Misc.benchmark(1000) do
     l = tp53.transcripts.length
    end
    assert l > 0
  end

  def __test_subset
    gene = "TP53"
    found = Genomics.knowledge_base.identify :pina, gene
    p53_interactors = Misc.profile{ Genomics.knowledge_base.children(:pina, found).target_entity }


    Misc.profile do
      puts Genomics.knowledge_base.subset(:pina,{"Gene" => p53_interactors}).length
    end
    ddd 2
    #assert Genomics.knowledge_base.subset(:pina,{"Gene" => p53_interactors}).target_entities.name.include? "MDM2"
  end

  def test_syndication
    kb = KnowledgeBase.new Rbbt.tmp.test.kb2, "Hsa/jan2013"
    kb.syndicate @kb, :genomics

    gene = "TP53"
    found = kb.identify "pina@genomics", gene
    assert found =~ /ENSG/
  end
end

