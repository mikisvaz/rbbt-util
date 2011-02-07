require File.join(File.expand_path(File.dirname(__FILE__)), 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/sources/organism'

class TestNameSpaces < Test::Unit::TestCase

  def test_file_namespaces
    assert_equal "Hsa", Rbbt.files.Organism.Hsa.gene_positions.namespace
    assert_equal "Hsa", Rbbt.files.Organism.Hsa.namespace
  end
 
  def test_tsv_namespaces
    assert_equal "Hsa", Rbbt.files.Organism.Hsa.gene_positions.tsv.namespace
    assert_equal "Hsa", TSV.new(Rbbt.files.Organism.Hsa.gene_positions).namespace
  end
  
  def test_field_namespace
    assert_equal ["Hsa"], Rbbt.files.Organism.Hsa.gene_positions.tsv.all_fields.collect{|f| f.namespace}.uniq
  end

  def test_attach_tracks_namespace
    require 'rbbt/sources/matador'
    gene_pos = TSV.new(Rbbt.files.Organism.Hsa.gene_positions)

    gene_pos.attach Matador.protein_drug

    assert gene_pos.fields.include? "Matador:Chemical"
  end
 
end
