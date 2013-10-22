require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/workflow'
require 'rbbt/association'
require 'rbbt/association/index'

class TestAssociationIndex < Test::Unit::TestCase

  def setup
    Workflow.require_workflow "Genomics"
    require 'rbbt/entity/gene'
  end

  def teardown
  end


  def test_subset
    require 'rbbt/sources/pina'
    require 'rbbt/sources/kegg'
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013").ensembl
    index = Association.index(Pina.protein_protein,
                              {:namespace => tp53.organism, 
                                :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession",
                                :format => "Ensembl Gene ID",
                                :undirected => true}, 
                                {:dir => '/tmp/test_association3', :update => false})
    genes = tp53.pina_interactors.ensembl
    genes << tp53

    Misc.benchmark(10) do
      index.subset_entities("Ensembl Gene ID" => genes).length
    end

    assert_equal 204, index.subset_entities("Ensembl Gene ID" => genes).select{|m| m.partition("~")[0] == tp53}.uniq.length
  end
end
