require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/tmpfile'
require 'rbbt/util/tc_hash'

class TestTCHash < Test::Unit::TestCase
  def test_each
    TmpFile.with_file do |f|
      t = TCHash.new f
      t["1"] = 2
      t["2"] = 3

      t.collect do |k,v| 
        ["1", "2"].include? k 
      end
    end
  end

  def test_serializer
    TmpFile.with_file do |f|
      t = TCHash.new f, TCHash::StringSerializer
      t["1"] = 2
      t["2"] = 3
      t.read

      t.collect do |k,v| 
        ["1", "2"].include? k 
      end
    end
  end

  def test_stringArraySerializer
    TmpFile.with_file do |f|
      t = TCHash.get f, true, TCHash::StringArraySerializer
      t["1"] = [1,2]
      t["2"] = [3,4]

      t = TCHash.get f
      t.collect do |k,v| 
        assert_equal ["1", "2"], t["1"] 
      end
    end
  end

  def test_stringDoubleArraySerializer
    TmpFile.with_file do |f|
      t = TCHash.get f, true, TCHash::StringDoubleArraySerializer
      t["1"] = [[1],[2]]
      t["2"] = [[3],[4,5]]

      t = TCHash.get f
      t.collect do |k,v| 
       assert_equal [["3"],["4","5"]], t["2"]
      end
    end
  end

  def test_serializer_alias
    TmpFile.with_file do |f|
      t = TCHash.get f, true, :double
      t["1"] = [[1],[2]]
      t["2"] = [[3],[4,5]]

      t = TCHash.get f
      assert_equal [["3"],["4","5"]], t["2"]
    end
  end

  def test_non_blocking
    TmpFile.with_file do |f|
      t = TCHash.get f, true, :double
      t["1"] = [[1],[2]]
      t["2"] = [[3],[4,5]]
      t.close

      pid = Process.fork do
        t2 = TCHash.get f, true, :double
        assert_equal [["3"],["4","5"]], t2["2"]
        exit
      end

      t2 = TCHash.get f, true, :double
      assert_equal [["3"],["4","5"]], t2["2"]

      Process.wait pid
    end
  end
end

