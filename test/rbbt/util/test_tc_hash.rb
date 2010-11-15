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
        puts k 
      end
    end
  end
end

