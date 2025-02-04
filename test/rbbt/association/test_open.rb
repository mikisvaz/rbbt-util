require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'rbbt/association'
require 'rbbt/association/open'

require 'rbbt/tsv'
require 'rbbt/persist'

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
    :identifiers => datadir_test.identifiers
  }

  #EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup

  def test_open_no_persist
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.database(f, **EFFECT_OPTIONS)
      assert_equal ["false"], tsv["MDM2"]["directed?"]
    end
  end


  def test_open_persist
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.database(f, **EFFECT_OPTIONS.merge(:persist => true))
      assert_equal "ENSG00000141510", tsv["MDM2"]["Ensembl Gene ID"].first
      assert_equal ["false"], tsv["MDM2"]["directed?"]
    end
  end

  def test_open_no_persist_string
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.database(f, **EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~SG (Associated Gene Name)", :persist => false))
      tsv.include? "TP53"
      assert_equal ["MDM2"], tsv["TP53"]["SG"]
    end
  end

  def test_index_no_persist_string
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.index(f, **EFFECT_OPTIONS.merge(:source => "TG", :target => "SG=~Associated Gene Name"))
      tsv.unnamed = false
      assert_equal "inhibition", tsv["TP53~MDM2"]["Effect"]
    end
  end

  def test_index_persist_string
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.index(f, **EFFECT_OPTIONS.merge(:source => "SG", :target => "TG=~Associated Gene Name"))
      tsv.unnamed = false
      assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
      assert_equal %w(TP53~GLI1 TP53~NFKB1), tsv.match("TP53").sort
    end
  end

  def test_index_persist_reverse
    TmpFile.with_file(EFFECT) do |f|
      tsv = Association.index(f, **EFFECT_OPTIONS.merge( :source => "TG", :target => "SG=~Associated Gene Name"), :persist => true).reverse
      tsv.unnamed = false
      assert_equal "inhibition", tsv["MDM2~TP53"]["Effect"]
      assert_equal %w(MDM2~TP53), tsv.match("MDM2")
    end
  end
end
