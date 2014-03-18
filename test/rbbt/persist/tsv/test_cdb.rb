require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSVCDB < Test::Unit::TestCase

  if Persist.respond_to? :open_cbd
    def test_organism
      require 'rbbt/sources/organism'
      TmpFile.with_file nil do |tmp_file|
        file = CMD.cmd("head -n 1000000", :in => Organism.identifiers("Hsa").open, :pipe => true)
        tsv = Organism.identifiers("Hsa").tsv(:key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "CDB", :persist_dir => tmp_file)
        assert_equal "ENSG00000141510", tsv["TP53"]
      end
    end
  end
end
