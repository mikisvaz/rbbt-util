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

  def test_shard_fwt
    TmpFile.with_file do |dir|
      shard_function = Proc.new do |key|
        key[0..(key.index(":")-1)]
      end

      pos_function = Proc.new do |key|
        key.split(":").last.to_i
      end

      size = 10
      sharder = Persist.persist_tsv(nil, "ShardTest", {}, :update => true, :range => false, :value_size => 64, :engine => 'fwt', :file => dir, :shard_function => shard_function, :pos_function => pos_function, :persist => true, :serializer => :float) do |db|
        size.times do |v| 
          v = v + 1
          chr = "chr" << (v % 5).to_s
          key = chr + ":" << v.to_s
          db << [key, v*2]
        end
      end
      sharder.read

      assert_equal dir, sharder.persistence_path
      assert_equal size, sharder.size

      assert_equal [4.0], sharder["chr2:2"]

      count = 0
      sharder.through do |k,v|
        count += 1
      end
      assert_equal count, size

      sharder = Persist.open_sharder(dir, false, :float, 'fwt', {:range => false, :value_size => 64, :pos_function => pos_function}, &shard_function)

      assert_equal [4.0], sharder["chr2:2"]

      assert_equal size, sharder.size 
    end
  end
end

