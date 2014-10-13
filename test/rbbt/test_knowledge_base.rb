require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/workflow'
require 'rbbt/entity'
require 'rbbt/entity/identifiers'

require 'rbbt/association'
require 'rbbt/knowledge_base'

require 'rbbt/sources/organism'
require 'rbbt/sources/tfacts'
require 'rbbt/sources/kegg'

module Gene
  extend Entity
  add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
  add_identifiers KEGG.identifiers

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

  def test_knowledge_base
    organism = Organism.default_code("Hsa")
    TmpFile.with_file do |tmpdir|
      kb = KnowledgeBase.new tmpdir, Organism.default_code("Hsa")
      kb.format = {"Gene" => "Ensembl Gene ID"}

      kb.register :tfacts, TFacts.regulators, :source =>"=~Associated Gene Name"

      assert_equal "Ensembl Gene ID", kb.get_database(:tfacts).key_field

      kb.register :kegg, KEGG.gene_pathway, :source_format => "Ensembl Gene ID"
      assert_match "Ensembl Gene ID", kb.get_database(:kegg).key_field

      gene = Gene.setup("TP53", "Associated Gene Name", organism)
      assert_equal "TP53", gene.name
      assert_equal "ENSG00000141510", gene.ensembl

      downstream = gene.follow kb, :tfacts
      upstream = gene.backtrack kb, :tfacts
      close = gene.expand kb, :tfacts

      assert downstream.length < downstream.follow(kb, :tfacts,false).flatten.length
      assert downstream.follow(kb, :tfacts,false).flatten.length < Annotated.flatten(downstream.follow(kb, :tfacts)).follow(kb, :tfacts).flatten.length

      Misc.benchmark(50) do
        downstream.follow(kb, :tfacts,false)
        downstream.backtrack(kb, :tfacts,false)
        downstream.expand(kb, :tfacts,false)
      end

      Misc.benchmark(50) do
        downstream.follow(kb, :tfacts)
        downstream.backtrack(kb, :tfacts)
        downstream.expand(kb, :tfacts)
      end
      
      Misc.benchmark(50) do
        downstream.follow(kb, :tfacts, true)
        downstream.backtrack(kb, :tfacts, true)
        downstream.expand(kb, :tfacts, true)
      end
    end
  end
end

