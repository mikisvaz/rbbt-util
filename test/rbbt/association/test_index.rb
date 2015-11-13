require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'rbbt/association'
require 'rbbt/association/index'

class TestAssociationIndex < Test::Unit::TestCase
   
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

  def test_index_no_persist_string
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"), :persist => false)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["TP53~MDM2"]["Effect"]
  end

  def test_index_no_persist_string_undirected
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => true, :source => "TG", :target => "SG=~Associated Gene Name"), :persist => false)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["TP53~MDM2"]["Effect"]
  end

  def test_index_persist_string
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "SG", :target => "TG=~Associated Gene Name"), :persist => true, :update => true)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
    assert_equal %w(TP53~NFKB1 TP53~GLI1).sort, tsv.match("TP53").sort
  end

  def test_index_persist_reverse
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"), :persist => true, :update => true).reverse
    tsv.unnamed = false
    assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
    assert_equal %w(MDM2~TP53), tsv.match("MDM2")
  end

  def test_index_persist_undirected
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => true, :source => "TG=~Associated Gene Name", :target => "SG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
    assert_equal "inhibition", tsv["TP53~MDM2"]["Effect"]
    assert_equal %w(MDM2~TP53), tsv.match("MDM2")
  end

  def test_index_persist_directed_subset
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => true, :source => "TG=~Associated Gene Name", :target => "SG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    assert_equal %w(TP53~GLI1 TP53~MDM2).sort, tsv.subset(["TP53"], ["GLI1","MDM2"]).sort
    assert_equal %w(MDM2~TP53).sort, tsv.subset(["MDM2"], :all).sort
    assert_equal %w(GLI1~TP53).sort, tsv.subset(["GLI1"], :all).sort
    assert_equal %w(TP53~GLI1).sort, tsv.subset(:all, ["GLI1"]).sort
  end

  def test_index_persist_undirected_subset
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => false, :source => "SG=~Associated Gene Name", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    assert_equal %w(TP53~GLI1 TP53~NFKB1).sort, tsv.subset(["TP53"], ["GLI1","MDM2", "NFKB1"]).sort
  end

  def test_index_flat
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(file,  :type => :flat, :source => "Transcription Factor Associated Gene Name=~Associated Gene Name", :merge => true)
    assert tsv.match("TP53").length > 10
  end

  def test_index_flat_to_matrix
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(file,  :type => :flat, :source => "Transcription Factor Associated Gene Name=~Associated Gene Name", :merge => true)
    assert(tsv.to_matrix(false))
  end

  def test_filter_no_block
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => false, :source => "SG=~Associated Gene Name", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    matches = tsv.filter :directed?
    assert_equal 2, matches.length
  end

  def test_filter_no_block_value
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => false, :source => "SG=~Associated Gene Name", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    matches = tsv.filter :Effect, "inhibition"
    assert_equal ["MDM2~TP53"], matches
  end

  def test_filter_block_value_field
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => false, :source => "SG=~Associated Gene Name", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    matches = tsv.filter :Effect do |value|
      return value.include? "inhibition"
    end
    assert_equal ["MDM2~TP53"], matches
  end

  def test_filter_block_no_value_field
    require 'rbbt/sources/tfacts'
    file = TFacts.regulators
    tsv = Association.index(EFFECT, EFFECT_OPTIONS.merge(:undirected => false, :source => "SG=~Associated Gene Name", :target => "TG=~Associated Gene Name"), :persist => true)
    tsv.unnamed = false
    matches = tsv.filter do |key,values|
      return values.flatten.include? "inhibition"
    end
    assert_equal ["MDM2~TP53"], matches
  end
end
