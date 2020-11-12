require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestPersistTSVCDB < Test::Unit::TestCase

  if Persist.respond_to? :open_cbd
    def test_organism
      require 'rbbt/sources/organism'
      TmpFile.with_file nil do |tmp_file|
        tsv = Organism.identifiers("Hsa").tsv(:key_field => "Associated Gene Name", :fields => ["Ensembl Gene ID"], :type => :single, :persist => true, :persist_engine => "CDB", :persist_dir => tmp_file)
        assert_equal "ENSG00000141510", tsv["TP53"]
      end
    end
  end
end
