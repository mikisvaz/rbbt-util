require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'test/unit'
require 'rbbt/persist/tsv'

class TestSharder < Test::Unit::TestCase
  def test_shard
    TmpFile.with_file do |dir|
      sharder = Persist::Sharder.new dir, true, :float_array, 'HDB' do |key|
        key[-1]
      end

      keys = []
      size = 1_000_000
      Misc.benchmark(2) do
        sharder.write_and_read do
          size.times do |v| 
            keys << v.to_s
            sharder[v.to_s] = [v, v*2]
          end
        end

        assert_equal size, sharder.keys.length
      end
    end
  end
end

