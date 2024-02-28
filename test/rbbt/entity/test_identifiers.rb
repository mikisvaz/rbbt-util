require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/entity'
require 'rbbt/entity/identifiers'

module Gene
  extend Entity
end

class TestEntityIdentifiers < Test::Unit::TestCase

  def test_name
    Gene.add_identifiers datafile_test('identifiers'), "Ensembl Gene ID", "Associated Gene Name"
    assert_equal "TP53", Gene.setup("ENSG00000141510").name
  end

  def test_name_organism
    keyword_test :organism do
      require 'rbbt/sources/organism'
      Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
      assert_equal "TP53", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").name
    end
  end

  def test_identifier_files
    Gene.add_identifiers datafile_test('identifiers'), "Ensembl Gene ID", "Associated Gene Name"
    assert Gene.identifier_files.any?
  end

  def test_Entity_identifier_files
    Gene.add_identifiers datafile_test('identifiers'), "Ensembl Gene ID", "Associated Gene Name"
    assert Entity.identifier_files("Ensembl Gene ID").any?
  end
end
