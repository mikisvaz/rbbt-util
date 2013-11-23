require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/workflow'
require 'rbbt/entity'

require 'rbbt/association'
require 'rbbt/knowledge_base'

gem 'rbbt-sources'
gem 'rbbt-phgx'

Log.severity=0
Workflow.require_workflow "Genomics"
require 'rbbt/entity/gene'
require 'rbbt/sources/pina'

TEST_ASSOCIATIONS =<<-EOF
#: :sep=" "#:namespace=Hsa/jan2013
#Entity1 Entity2 Score Entity3 Gene
a A 1 aa TP53
b B 2 bb KRAS
c C|K 3|4 cc|kk PTEN|PTEN
EOF

class TestAssociations < Test::Unit::TestCase
  
  FAssocs = ""
  DAssocs = ""

  def setup
    FAssocs.replace TmpFile.tmp_file
    DAssocs.replace TmpFile.tmp_file
    Open.write(FAssocs, TEST_ASSOCIATIONS)
  end

  def teardown
    FileUtils.rm FAssocs
    FileUtils.rm_rf DAssocs
  end

  def test_simple_open
    database = Association.open(FAssocs, {}, :dir => DAssocs)
    assert_equal ["C", "K"], database["c"]["Entity2"]
  end
 
  def test_source_open
    database = Association.open(FAssocs, {:source => "Entity2", :zipped => true}, :dir => DAssocs)
    assert_equal ["c", "3", 'cc', "PTEN"], database["C"].flatten
    assert_equal ["c", "4", 'kk', "PTEN"], database["K"].flatten
  end
  
  def test_target_open
    database = Association.open(FAssocs, {:source => "Entity2", :target => "Entity3", :zipped => true}, :dir => DAssocs)
    assert_equal ["cc", "c", "3", "PTEN"], database["C"].flatten
    assert_equal ["kk", "c", "4", "PTEN"], database["K"].flatten
  end

  def test_target_open

    database = Association.open(Pina.protein_protein,{ 
                                :undirected => false, 
                                :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession"}, :persist => false)

    assert( ! database["Q13547"][0].include?("O15379"))

    database = Association.open(Pina.protein_protein,{
                                :undirected => true, 
                                :target => "Interactor UniProt/SwissProt Accession=~UniProt/SwissProt Accession"}, :persist => false)

    assert database["O15379"][0].include? "Q13547"

    assert database["Q13547"][0].include? "O15379"
  end


  def test_gene_open
    database = Association.open(FAssocs, {:source => "Gene=~Associated Gene Name", :target => "Entity3", :zipped => true}, :dir => DAssocs)
    assert_equal ["aa"], database["TP53"].first
  end

  def test_gene_open_translate
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013")
    database = Association.open(FAssocs, {:source => "Gene=~Associated Gene Name", :source_format => "Ensembl Gene ID", :target => "Entity3", :zipped => true}, :dir => DAssocs)
    assert_equal ["aa"], database[tp53.ensembl].first
  end

  def test_gene_target_open_translate
    tp53 = Gene.setup("TP53", "Associated Gene Name", "Hsa/jan2013")
    database = Association.open(FAssocs, {:target => "Gene=~Associated Gene Name=>Ensembl Gene ID", :source => "Entity3", :zipped => true}, :dir => DAssocs)
    assert_equal [tp53.ensembl], database["aa"].first
  end

  def test_ICGC
    assoc = Association.open(Open.open('ftp://data.dcc.icgc.org/current/Chronic_Lymphocytic_Leukemia-ISC_MICINN-ES/simple_somatic_mutation.CLLE-ES.tsv.gz'),
                         { :source => "gene_affected=~Ensembl Gene ID=>Associated Gene Name", :target => "icgc_donor_id=~Sample", 
                           :fields => ['consequence_type'],  
                           :namespace => 'Hsa/jan2013',
                           :merge => true, :header_hash=>''}, :persist => false)

    assert_equal 9, assoc["SF3B1"]["Sample"].uniq.length

  end


end
