require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/workflow'
require 'rbbt/entity'
require 'rbbt/entity/identifiers'

require 'rbbt/association'
require 'rbbt/knowledge_base'

module Gene
  extend Entity
  property :follow => :single do |kb,name,annotate=nil|
    if annotate.nil? or annotate
      l = kb.children(name, self).target_entity
      self.annotate l if annotate and kb.source(name) == format
      l
    else
      kb._children(name, self).collect{|v| v.partition("~").last }
    end
  end

  property :backtrack => :single do |kb,name,annotate=nil|
    if annotate.nil? or annotate
      l = kb.parents(name, self).target_entity
      self.annotate l if annotate and kb.target(name) == format
      l
    else
      kb._parents(name, self).collect{|v| v.partition("~").last }
    end
  end

  property :expand => :single do |kb,name,annotate=nil|
    if annotate.nil? or annotate
      n = kb.neighbours(name, self)
      if kb.source(name) == kb.target(name) 
        self.annotate n.collect{|k,v| v.target}.flatten
      else
        n.collect{|k,v| v.target_entity.to_a}.flatten
      end
    else
      n = kb._neighbours(name, self)
      n.values.flatten.collect{|v| v.partition("~").last}
    end
  end
end


class TestKnowledgeBase < Test::Unit::TestCase
  def setup
    require 'rbbt/sources/organism'

    Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
  end

  def test_knowledge_base_simple
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :gene_ages, datadir_test.gene_ages

      i = kb.get_index(:gene_ages)

      assert_include i.match("ENSG00000000003"), "ENSG00000000003~Bilateria"
    end
  end

  def test_knowledge_base_translate
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :gene_ages, datadir_test.gene_ages, :source => "FamilyAge", :target => "=>Associated Gene Name"

      i = kb.get_index(:gene_ages)

      assert_include i.match("Bilateria"), "Bilateria~SMAD4"
    end
  end


  def test_knowledge_base_reverse
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :gene_ages, datadir_test.gene_ages

      ri = kb.get_index(:gene_ages).reverse

      assert_include ri.match("Bilateria"), "Bilateria~ENSG00000000003"
    end
  end

  def test_entity
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :gene_ages, datadir_test.gene_ages, :source => "=>Associated Gene Name"

      kb.register :CollecTRI, datadir_test.CollecTRI, 
        :source => "Transcription Factor", :target => "Target Gene",
        :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"]

      smad4 = Gene.setup("SMAD4", "Associated Gene Name", kb.namespace)
      smad7 = Gene.setup("SMAD7", "Associated Gene Name", kb.namespace)


      assert_include smad4.follow(kb, :CollecTRI), smad7
      assert_include smad7.backtrack(kb, :CollecTRI), smad4
      refute smad7.follow(kb, :CollecTRI).include?(smad4)
      assert_include smad7.expand(kb, :CollecTRI), smad4
      assert_include smad4.expand(kb, :CollecTRI), smad7

    end
  end

  def __test_benchmark
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :gene_ages, datadir_test.gene_ages, :source => "=>Associated Gene Name"

      kb.register :CollecTRI, datadir_test.CollecTRI, 
        :source => "Transcription Factor", :target => "Target Gene (Associated Gene Name)",
        :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"]


      smad4 = Gene.setup("SMAD4", "Associated Gene Name", kb.namespace)
      downstream = smad4.follow(kb, :CollecTRI, true)
      Gene.setup(downstream)

      downstream.follow(kb, :CollecTRI)
      downstream.backtrack(kb, :CollecTRI)
      downstream.expand(kb, :CollecTRI)

      Misc.benchmark(50) do
        downstream.follow(kb, :CollecTRI)
        downstream.backtrack(kb, :CollecTRI)
        downstream.expand(kb, :CollecTRI)
      end
      
      Misc.benchmark(50) do
        downstream.follow(kb, :CollecTRI, true)
        downstream.backtrack(kb, :CollecTRI, true)
        downstream.expand(kb, :CollecTRI, true)
      end
    end
  end

  def test_identifier_files
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      Path.setup(tmpdir)
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")

      kb.register :gene_ages, datadir_test.gene_ages

      assert_include kb.get_database(:gene_ages).identifier_files.first, "test/data"
      assert_include kb.get_index(:gene_ages).identifier_files.first, "test/data"

    end
  end

  def test_knowledge_base_reuse
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      Path.setup(tmpdir)
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.register :CollecTRI, datadir_test.CollecTRI,
        :source => "Transcription Factor=~Associated Gene Name=>Ensembl Gene ID", :target => "Target Gene",
        :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"]

      assert kb.get_database(:CollecTRI).identifier_files.any?

      i = Association.index(datadir_test.CollecTRI, :persist_file => tmpdir.CollecTRI, 
                        :source => "Transcription Factor=~Associated Gene Name=>Ensembl Gene ID", :target => "Target Gene",
                        :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"],
                        :format => {"Gene" => "Ensembl Gene ID"},
                        :namespace => Organism.default_code("Hsa"))

      assert i.identifier_files.any?

      kb = KnowledgeBase.load(tmpdir)

      assert kb.get_database(:CollecTRI).identifier_files.any?

      i =  kb.get_index(:CollecTRI)

      assert i.identifier_files.any?
      assert kb.identify_source('CollecTRI', "SMAD4") =~ /ENSG/
    end
  end


  def test_flat
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kbfile = File.join(tmpdir, 'kb')
      file = File.join(tmpdir, 'file')
      kb = KnowledgeBase.new kbfile
      kb.register :test_flat do
        str =<<-EOF
#: :type=:flat#:sep=' '
#Key Value
a b c d e
A B C D E
        EOF
        Open.write(file, str)
        file
      end
      db = kb.get_database(:test_flat)
      assert db["a"].first.length > 1
    end
  end
end

