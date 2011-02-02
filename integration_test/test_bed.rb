require File.join(File.expand_path(File.dirname(__FILE__)), 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/sources/organism'

class TestBed < Test::Unit::TestCase
  def test_tsv_speed
    data = nil
    profile do
      data = TSV.new test_datafile("Metastasis.tsv"), :unique=> true
    end

    profile do
      data.slice "Chromosome Name" 
    end
  end

  def test_index
    index = Organism.Hsa.identifiers.index 
    index = Organism.Hsa.identifiers.index 
    assert_equal "1020", Misc.first(index["CDK5"])
  end

  def test_bed_speed
    require 'rbbt/sources/organism'
    data = nil

    data = TSV.new test_datafile("Metastasis.tsv"), :type=> :list, :key => "Position"

    chromosome_bed = {}

    CacheHelper.marshal_cache('bed_files') do
      positions = TSV.new Organism::Hsa.gene_positions, :list

      %w(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y).collect do |chromosome|
        subset = positions.select("Chromosome Name" => chromosome)
        [chromosome, Bed.new(subset, :range => ["Gene Start", "Gene End"], :value => "Entrez Gene ID", :persistence => true).persistence_file]
      end
    end.each{|chromosome, persistence_file| chromosome_bed[chromosome] = Bed.new(nil, :persistence_file => persistence_file)}

    benchmark do
      data.add_field "Entrez Gene ID" do |position, values|
        values["Chromosome Name"].collect{|chromosome|
          chromosome_bed[chromosome].nil? ? [] : chromosome_bed[chromosome][position]
        }.flatten
      end
    end


    benchmark do
      data.add_field "Ensembl Gene ID" do |position, values|
        Organism::Hsa.normalize(values["Entrez Gene ID"], :field => "Ensembl Gene ID")
      end
    end

    benchmark do
      data.add_field "Associated Gene Name" do |position, values|
        Organism::Hsa.normalize(values["Entrez Gene ID"], :field => "Associated Gene Name")
      end
    end
  end

  def test_namespace_identifiers
     assert_equal Rbbt.files.Organism.Hsa.identifiers, Rbbt.files.Organism.Hsa.gene_positions.namespace_identifiers.first
  end

  def test_index
    i = nil
    profile false do
      i = Organism.Hsa.identifiers.index :persistence => true, :persistence_update => true, :order => false, :target => "Associated Gene Name"
    end

    assert i.case_insensitive
    assert i["1020"].include? "CDK5"
  end

  def test_organism
    Organism.Hsa.identifiers2.index :target => "Ensembl Protein ID", :persistence => false
  end

  def test_NGS
    require 'rbbt/sources/kegg'
    require 'rbbt/sources/pharmagkb'

    human = Organism::Hsa
    p PhGx.files.KEGG.gene_pathway.tsv_fields

    #data = TSV.new test_datafile("Metastasis.tsv"), :type=> :list, :key => "Position"
  end
end

