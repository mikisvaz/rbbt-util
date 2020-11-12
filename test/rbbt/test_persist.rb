require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

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
      tsv = Persist.persist("Dumper", :tsv, :dir => tmpdir) do
        dumper = TSV::Dumper.new :key_field => "Field 1", :fields => ["Field 2"], :type => :single

        dumper.init
        Thread.new do
          10.times do |i|
            key = i.to_s
            dumper.add key, key + " - 2"
          end
          dumper.close
        end
        dumper
      end

      assert_equal 10, tsv.size
    end
  end

  def test_tsv_dumper_stream
    TmpFile.with_file do |tmpdir|
      stream = Persist.persist("Dumper", :tsv, :dir => tmpdir, :no_load => :stream) do
        dumper = TSV::Dumper.new :key_field => "Field 1", :fields => ["Field 2"], :type => :single

        Thread.new do
          10.times do |i|
            key = i.to_s
            dumper.add key, key + " - 2"
          end
          dumper.close
        end

        dumper
      end

      assert_equal 10, stream.read.split("\n").length
      stream.join
    end
  end
end
