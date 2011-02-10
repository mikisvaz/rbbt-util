require File.join(File.expand_path(File.dirname(__FILE__)), 'test_helper.rb')
require 'rbbt-util'

class TestWorkflow < Test::Unit::TestCase

  def test_NGS_workflow
    WorkFlow.run do 
      require 'rbbt/sources/organism'
      require 'rbbt/sources/kegg'
      require 'rbbt/sources/pharmagkb'
      require 'rbbt/sources/matador'
      require 'rbbt/sources/nci'

      data = TSV.new test_datafile("Metastasis2.tsv"), :type=> :double, :key => "Position"

      step :annotate_positions do
        chromosome_bed = {}

        %w(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y).collect do |chromosome|
          chromosome_bed[chromosome] = Persistence.persist(Organism::Hsa.gene_positions, "Gene_positions[#{chromosome}]", :fwt, :chromosome => chromosome, :range => true) do |file, options|
            tsv = file.tsv(:persistence => true, :type => :list)
            tsv.select("Chromosome Name" => chromosome).collect do |gene, values|
              [gene, values.values_at("Gene Start", "Gene End").collect{|p| p.to_i}]
            end
          end
        end

        data.add_field "Ensembl Gene ID" do |position, values|
          chromosome = values["Chromosome Name"].first
          next if chromosome_bed[chromosome].nil?
          chromosome_bed[chromosome][position]
        end

        data
      end

      step :add_identifiers do
        data.identifiers = Organism::Hsa.identifiers

        data
      end

      step :attach_info do

        data.attach KEGG.gene_pathway
        data.attach Matador.protein_drug
        data.attach PharmaGKB.gene_pathway
        data.attach PharmaGKB.gene_drug
        data.attach NCI.gene_drug
        data.attach NCI.gene_cancer

        data
      end
    end
  end
end

