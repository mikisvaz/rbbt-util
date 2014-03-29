require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersist < Test::Unit::TestCase

  def test_array_persist
    TmpFile.with_file do |tmp|
      10.times do
        assert_equal ["1", "2"],(Persist.persist("Test", :array, :file => tmp) do
          ["1", "2"]
        end)
      end
    end

    TmpFile.with_file do |tmp|
      10.times do
        assert_equal [],(Persist.persist("Test", :array, :file => tmp) do
          []
        end)
      end
    end

    TmpFile.with_file do |tmp|
      10.times do
        assert_equal ["1"],(Persist.persist("Test", :array, :file => tmp) do
          ["1"]
        end)
      end
    end
  end

  def test_tsv_dumper
    TmpFile.with_file do |tmpdir|
      stream = Persist.persist("Dumper", :tsv, :dir => tmpdir) do
        dumper = TSV::Dumper.new :key_field => "Field 1", :fields => ["Field 2"], :type => :single

        dumper.init
        Thread.new do
          10.times do |i|
            key = i.to_s
            dumper.add key, key + " - 2"
          end
          dumper.close
          Thread.exit
        end
        dumper
      end

      while line = stream.gets do
        puts line
      end

    end
  end
end
