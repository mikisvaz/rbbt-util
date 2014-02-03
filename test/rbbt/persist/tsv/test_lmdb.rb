require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

require 'rbbt/sources/organism'
module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSVLMDB < Test::Unit::TestCase

  def test_persist_lmbd
    Random.new
    test=nil
    TmpFile.with_file nil, false do |tmp_file|
      db = nil
      Misc.benchmark do
        db = Organism.identifiers("Hsa").tsv :fields => ["Associated Gene Name"], :persist => true, :persist_engine => "LMDB", :persist_dir => tmp_file
      end
      test = db.keys.sort{rand}[0..1000]
      Misc.benchmark do
        10.times do 
          test.each do |k| db[k] end
        end
      end
    end
    TmpFile.with_file nil, false do |tmp_file|
      db = nil
      Misc.benchmark do
        db = Organism.identifiers("Hsa").tsv :fields => ["Associated Gene Name"], :persist => true, :persist_engine => "BDB", :persist_dir => tmp_file
        fff db
      end
      Misc.benchmark do
        10.times do 
          test.each do |k| db[k] end
        end
      end
    end
  end
end
