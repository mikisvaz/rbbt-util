require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/sources/organism'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSVKC < Test::Unit::TestCase
  def _test_organism_kch
    require 'rbbt/sources/organism'
    TmpFile.with_file do |tmp_file|
      tsv = Organism.identifiers("Hsa").tsv :key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "kch", :persist_dir => tmp_file
      assert_equal "ENSG00000141510", tsv["TP53"]
    end
  end


  def test_organism_kct
    require 'rbbt/sources/organism'
    TmpFile.with_file do |tmp_file|
      tsv = Organism.identifiers("Hsa").tsv :key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "kct", :persist_dir => tmp_file
      assert_equal "ENSG00000141510", tsv["TP53"]
    end
  end
end
