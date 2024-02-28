require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/knowledge_base/traverse'
require 'rbbt/workflow'

class TestKnowledgeBaseTraverse < Test::Unit::TestCase
  def with_kb(&block)
    keyword_test :organism do
      require 'rbbt/sources/organism'
      organism = Organism.default_code("Hsa")
      TmpFile.with_file do |tmpdir|
        kb = KnowledgeBase.new tmpdir
        kb.namespace = organism
        kb.format = {"Gene" => "Associated Gene Name"}

        kb.register :gene_ages, datadir_test.gene_ages, :source => "=>Associated Gene Name"

        kb.register :CollecTRI, datadir_test.CollecTRI, 
          :source => "Transcription Factor=~Associated Gene Name", 
          :target => "Target Gene=~Associated Gene Name",
          :fields => ["[ExTRI] Confidence", "[ExTRI] PMID"]

        yield kb
      end
    end
  end

  def test_traverse_simple
    with_kb do |kb|
      rules = []
      rules << "SMAD4 gene_ages ?1"
      res =  kb.traverse rules
      assert_include res.first["?1"], "Bilateria"
    end
  end

  def test_traverse_CollecTRI
    with_kb do |kb|
      rules = []
      rules << "SMAD4 CollecTRI ?1 - '[ExTRI] Confidence=High'"
      res =  kb.traverse rules
      assert res.last.any?
    end
  end


  def test_traverse
    with_kb do |kb|
      rules = []
      rules << "?1 CollecTRI SMAD7"
      rules << "?1 gene_ages ?2"
      rules << "SMAD4 gene_ages ?2"
      res =  kb.traverse rules
      assert res.first["?1"].include? "MYC"
    end
  end

  def test_target
    with_kb do |kb|
      rules = []
      rules << "?target =CollecTRI SMAD7"
      rules << "?1 CollecTRI ?target"
      rules << "?1 gene_ages ?2"
      rules << "SMAD4 gene_ages ?2"
      res =  kb.traverse rules
      assert res.first["?1"].include? "MYC"
    end
  end

  def test_target_translate
    with_kb do |kb|
      rules = []
      rules << "?target =CollecTRI ENSG00000101665"
      rules << "?1 CollecTRI ?target"
      res =  kb.traverse rules
      assert res.first["?1"].include? "MYC"
    end
  end

  def test_target_attribute
    with_kb do |kb|
      rules = []
      rules << "?1 CollecTRI SMAD7"
      all =  kb.traverse rules

      rules = []
      rules << "?1 CollecTRI SMAD7 - '[ExTRI] Confidence'=High"
      low =  kb.traverse rules

      assert low.last.length < all.last.length
    end
  end

  def test_traverse_same_age
    with_kb do |kb|
      rules_str=<<-EOF
?target1 =gene_ages SMAD7
?target2 =gene_ages SMAD4
?target1 gene_ages ?age
?target2 gene_ages ?age
?1 gene_ages ?age
      EOF
      rules = rules_str.split "\n"
      res =  kb.traverse rules
      assert_include res.first["?1"], "MET"
    end
  end

  def test_traverse_same_age_acc
    with_kb do |kb|
      rules_str=<<-EOF
?target1 =gene_ages SMAD7
?target2 =gene_ages SMAD4
?age{
  ?target1 gene_ages ?age
  ?target2 gene_ages ?age
}
?1 gene_ages ?age
      EOF
      rules = rules_str.split "\n"
      res =  kb.traverse rules
      assert_include res.first["?1"], "MET"
    end
  end

  def test_wildcard_db
    with_kb do |kb|
      rules = []
      rules << "SMAD4 ?db ?1"
      res =  kb.traverse rules
    end
  end
end

