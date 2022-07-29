require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/entity'
require 'rbbt/entity/identifiers'

module Gene
  extend Entity
end

require 'rbbt/sources/kegg'
require 'rbbt/sources/organism'
Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
Gene.add_identifiers KEGG.identifiers
Gene.add_identifiers Organism.identifiers("NAMESPACE"), "Ensembl Gene ID", "Associated Gene Name"
Gene.add_identifiers KEGG.identifiers

class TestEntityIdentifiers < Test::Unit::TestCase
  def test_name
    Gene.add_identifiers datafile_test('identifiers'), "Ensembl Gene ID", "Associated Gene Name"
    assert_equal "TP53", Gene.setup("ENSG00000141510").name
  end

  def test_name_organism
    assert_equal "TP53", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").name
  end

  def test_identifier_files
    assert Gene.identifier_files.any?
  end

  def test_Entity_identifier_files
    assert Entity.identifier_files("Ensembl Gene ID").any?
  end

  def test_translate_kegg
    assert_match "hsa", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").to("KEGG Gene ID")
    assert_match "TP53", Gene.setup("ENSG00000141510", "Ensembl Gene ID", "Hsa/feb2014").to("KEGG Gene ID").to(:name)
  end

  def ___test_complex

    file = Path.setup('/home/mvazquezg/git/workflows/PanCancer/share/network/gene_sets/CORUM_protein_complexes.gmt')
    name = File.basename(file).sub(/\.gmt$/,'')
    organism = "Hsa/feb2014"
    key_field = "#{ name } Pathway ID"
    description_field = "#{name} Pathway Description"
    description_file = file.find + '.identifiers'
    tsv = TSV.open(file, :fix => Proc.new{|l| p=l.split"\t"; [p[0], p[1], p[2..-1]*"|"]*"\t"})
    tsv.namespace = organism
    tsv.unnamed = true
    gene_field, count = Organism.guess_id(organism, tsv.values.collect{|l| l.last}.flatten.uniq )
    tsv.key_field = key_field
    tsv.fields = [description_field, gene_field]
    descriptions = tsv.slice(description_field)
    Open.write(description_file, descriptions.to_single.to_s) #unless File.exist? description_file
    values = tsv.slice(gene_field)
    values.identifiers = description_file

    mod = Module.new
    mod_name = Misc.camel_case(key_field.gsub(/\s+/,'_').sub(/_ID$/,''))
    Object.const_set(mod_name, mod)
    mod.instance_eval do
      extend Entity
      add_identifiers Path.setup(description_file), key_field, description_field
      
      annotation :format
    end
    entity = "CORUM:6052"
    mod.setup(entity, :format => key_field)
    puts entity.name
  end
end
