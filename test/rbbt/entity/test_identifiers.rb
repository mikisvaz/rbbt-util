require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/entity'
require 'rbbt/entity/identifiers'

module Gene
  extend Entity
end

require 'rbbt/sources/kegg'
require 'rbbt/sources/organism'
Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
Gene.add_identifiers KEGG.identifiers
Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
Gene.add_identifiers KEGG.identifiers

class TestEntityIdentifiers < Test::Unit::TestCase
  def test_name
    Gene.add_identifiers datafile_test('identifiers'), "Ensembl Gene ID", "Associated Gene Name"
    assert_equal "TP53", Gene.setup("ENSG00000141510").name
  end

  def test_name_organism
    assert_equal "TP53", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").name
  end

  def test_identifier_files
    assert Gene.identifier_files.any?
  end

  def test_Entity_identifier_files
    assert Entity.identifier_files("Ensembl Gene ID").any?
  end

  def test_translate_kegg
    assert_match "hsa", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").to("KEGG Gene ID")
    assert_match "TP53", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").to("KEGG Gene ID").to(:name)
  end
end

