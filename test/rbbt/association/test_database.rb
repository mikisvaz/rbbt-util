require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt-util'
require 'rbbt/association'
require 'rbbt/association'
require 'rbbt/association/database'

class TestAssociationDatabase < Test::Unit::TestCase
   
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
  }

  EFFECT_TSV = TSV.open EFFECT, EFFECT_OPTIONS.dup
  def test_tsv
    assert_equal %w(NFKB1 GLI1), EFFECT_TSV["TP53"]["TG"]
  end

  EFFECT_DATABASE = Association.database(EFFECT_TSV, EFFECT_OPTIONS.dup)
  
  def test_open_new_tsv
    tsv = Association.database EFFECT_TSV, :fields => ["Effect"], :target => "TG=~Associated Gene Name=>Ensembl Gene ID"
    assert_equal ["Ensembl Gene ID", "Effect"], tsv.fields
    assert_equal "SG", tsv.key_field
    assert tsv.include? "MDM2"
  end

  def test_open_new_tsv_reverse
    tsv = Association.database EFFECT_TSV, :fields => ["Effect"], :target => "SG=~Associated Gene Name=>Ensembl Gene ID", :zipped => true
    assert_equal ["Ensembl Gene ID", "Effect"], tsv.fields
    assert_equal "TG", tsv.key_field
    assert tsv.include? "NFKB1"
  end

  def test_database_translate
    database = Association.database(EFFECT_TSV, EFFECT_OPTIONS.merge({:source => "SG=~Associated Gene Name=>Ensembl Gene ID", :target => "TG"}))
    assert_equal %w(inhibition), database["ENSG00000135679"]["Effect"]
    assert_equal %w(activation activation), database["ENSG00000141510"]["Effect"]
  end

  def test_database
    assert_equal %w(inhibition), EFFECT_DATABASE["MDM2"]["Effect"]
    assert_equal %w(activation activation), EFFECT_DATABASE["TP53"]["Effect"]
  end

  def test_index_list
    file = datafile_test('gene_ages')
    tsv = Association.database(file)
    assert_equal [["Bilateria"], ["Euteleostomi"], ["Duplicate"]], tsv["ENSG00000000003"]
  end

  def test_gene_ages
    assert Association.database('/home/mvazquezg/git/workflows/genomics/share/gene_ages', :source => "FamilyAge", :merge => true, :target => "Ensembl Gene ID", :persist => false).values.first.length > 1
  end

  def __test_reorder_multiple_mismatch

    text =<<-EOF
#: :filename=BLADDER_11_and_12#:namespace=Hsa/feb2014#:type=:double
#Genomic Mutation	Ensembl Gene ID	affected	damaged	splicing	broken	Sample
12:54423561:A	ENSG00000273049|ENSG00000197757|ENSG00000198353|ENSG00000273046	false|true|false|false	false|true|false|false	false|false|false|false	false|true|false|false	51106212
    EOF

    tsv = TSV.open(StringIO.new(text))
  
    index =  Association.index(tsv, :source => "Sample", :target => "Genomic Mutation", :merge => true, :persist_update => true)
    assert_equal 4, index["51106212~12:54423561:A"][0].split(";;").length

  end
end
