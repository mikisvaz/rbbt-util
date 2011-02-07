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

  def test_genes
    require 'rbbt/sources/organism'
    require 'rbbt/sources/kegg'
    require 'rbbt/sources/pharmagkb'

    data = TSV.new test_datafile('genes.txt'), :persistence => false
    data.key_field = "Associated Gene Name"
    data.fields = []
    data.identifiers = Organism::Hsa.identifiers

    data.attach KEGG.gene_pathway
    data.attach PharmaGKB.gene_pathway, ["Name", :key]

    data.add_field "SNPsandGO" do |key,values|
      SNPsandGO.predict(values["Unirprot ID"], values["Mutation"])
    end
    

    #i = Organism::Hsa.identifiers.index :fields => "Associated Gene Name", :target => "Ensembl Gene ID"
    #i = KEGG.identifiers.index :target => "KEGG Gene ID"
    #i = KEGG.identifiers.index :target => "KEGG Gene ID"

    #ddd i.keys.length

    puts data.to_s

  end

  def test_index
    index = Organism.Hsa.identifiers.index 
    index = Organism.Hsa.identifiers.index 
    assert_equal "1020", Misc.first(index["CDK5"])
  end

  def test_bed_speed
    require 'rbbt/sources/organism'
    require 'rbbt/sources/kegg'
    require 'rbbt/sources/pharmagkb'
    require 'rbbt/sources/matador'
    require 'rbbt/sources/nci'
    data = nil

    data = TSV.new test_datafile("Metastasis2.tsv"), :type=> :double, :key => "Position"

    chromosome_bed = {}

    %w(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y).collect do |chromosome|
      chromosome_bed[chromosome] = Persistence.persist(Organism::Hsa.gene_positions, "Gene_positions[#{chromosome}]", :fwt, :chromosome => chromosome, :range => true) do |file, options|
        tsv = file.tsv(:persistence => true, :type => :list)
        tsv.select("Chromosome Name" => chromosome).collect do |gene, values|
          [gene, values.values_at("Gene Start", "Gene End").collect{|p| p.to_i}]
        end
      end
    end

    benchmark do
      data.add_field "Ensembl Gene ID" do |position, values|
        chromosome = values["Chromosome Name"].first
        next if chromosome_bed[chromosome].nil?
        chromosome_bed[chromosome][position]
      end
    end

    data

    data.identifiers = Organism::Hsa.identifiers

    #Organism::Hsa.attach_translations data, "Ensembl Gene ID"

    #Organism::Hsa.attach_translations data, "Associated Gene Name"

    data.attach KEGG.gene_pathway
    data.attach Matador.protein_drug
    data.attach PharmaGKB.gene_pathway
    data.attach PharmaGKB.gene_drug
    data.attach NCI.gene_drug
    data.attach NCI.gene_cancer

    #puts data.to_s
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

    data = TSV.new test_datafile("Metastasis.tsv"), :type=> :list, :key => "Position"
    data.identifiers = Organism::Hsa.identifiers
    data.attach KEGG.gene_pathway
  end
end

