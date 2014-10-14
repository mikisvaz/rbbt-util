require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'rbbt/association'
require 'rbbt/association/open'

class TestAssociationOpen < Test::Unit::TestCase
   
  EFFECT =<<-END
#: :sep=" "#:type=:double
#SG TG Effect directed?
MDM2 TP53 inhibition false
TP53 NFKB1|GLI1 activation|activation true|true
  END

  EFFECT_OPTIONS = {
    :source => "SG=~Associated Gene Name",
    :target => "TG=~Associated Gene Name=>Ensembl Gene ID",
    :persist => false,
    :identifiers => datafile_test('identifiers'),
    :namespace => "Hsa"
  }

  EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup

  def test_open_no_persist
    tsv = Association.open(EFFECT_TSV, EFFECT_OPTIONS, :persist => false)
    assert_equal "ENSG00000141510", tsv["MDM2"]["Ensembl Gene ID"].first
    assert_equal ["false"], tsv["MDM2"]["directed?"]
  end


  def test_open_persist
    tsv = Association.open(EFFECT_TSV, EFFECT_OPTIONS, :persist => true, :update => true)
    assert_equal "ENSG00000141510", tsv["MDM2"]["Ensembl Gene ID"].first
    assert_equal ["false"], tsv["MDM2"]["directed?"]
  end

  def test_open_no_persist_string
    tsv = Association.open(EFFECT, EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"), :persist => false)
    tsv.include? "TP53"
    assert_equal ["MDM2"], tsv["TP53"]["Associated Gene Name"]
  end

  def test_index_no_persist_string
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"), :persist => false)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["TP53~MDM2"]["Effect"]
  end

  def test_index_persist_string
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "SG", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
    assert_equal %w(TP53~GLI1 TP53~NFKB1), tsv.match("TP53").sort
  end

  def test_index_persist_reverse
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"), :persist => true).reverse
    tsv.unnamed = false
    assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
    assert_equal %w(MDM2~TP53), tsv.match("MDM2")
  end
end
