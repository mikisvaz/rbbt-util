require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'test/unit'
require 'rbbt-util'
require 'rbbt/persist/tsv'

class TestSharder < Test::Unit::TestCase
  def test_shard
    TmpFile.with_file do |dir|
      shard_function = Proc.new do |key|
        key[-1]
      end

      size = 10
      sharder = Persist.persist_tsv(nil, "ShardTest", {}, :file => dir, :shard_function => shard_function, :persist => true, :serializer => :float_array) do |db|
        size.times do |v| 
          db[v.to_s] = [v, v*2]
        end
        db
      end
      assert_equal dir, sharder.persistence_path
      assert_equal size, sharder.keys.length

      assert_equal [2,4], sharder["2"]
      count = 0
      sharder.through do |k,v|
        count += 1
      end
      assert_equal count, size

      sharder = Persist::Sharder.new dir do |key|
        key[-1]
      end

      assert_equal size, sharder.keys.length

    end
  end
end

