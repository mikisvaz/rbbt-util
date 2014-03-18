require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSVLMDB < Test::Unit::TestCase

  def test_organism
    require 'rbbt/sources/organism'
    if Persist.respond_to? :open_lmdb
      TmpFile.with_file do |tmp_file|
        tsv = Organism.identifiers("Hsa").tsv :key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "LMDB", :persist_dir => tmp_file
        assert_equal "ENSG00000141510", tsv["TP53"]
      end
    end
  end
end
