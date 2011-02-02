require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt'
require 'rbbt/util/path'
require 'rbbt/sources/organism'

class TestPath < Test::Unit::TestCase
  def test_namespace
    assert_equal "Rbbt", Rbbt.files.foo.namespace
  end
 
  def test_indentifier_files
    assert_equal [Rbbt.files.Organism.Hsa.identifiers], Rbbt.files.Organism.Hsa.gene_positions.identifier_files
  end

 
  def test_indentifier_files2
    assert_equal [Rbbt.files.Organism.Hsa.identifiers], Organism::Hsa.gene_positions.identifier_files
  end
end

