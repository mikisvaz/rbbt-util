require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'scout/tsv'
require 'scout/tmpfile'
require 'test/unit'
require 'rbbt/tsv/util'

class TestTSVUtil < Test::Unit::TestCase

  def test_field_counts
   content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)

      assert_equal 2, TSV.field_match_counts(tsv, ["a","A","a","b","Id3"])["ValueA"]
      assert_equal nil, TSV.field_match_counts(tsv, ["ValueA"])["ValueA"]
    end
  end

  def test_replicates
   content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|bb|bbb    Id1|Id2|Id3
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)

      assert_equal 4, tsv.unzip_replicates.length
      assert_equal %w(aa bb Id2), tsv.unzip_replicates["row1(1)"]
    end
  end

  def test_merge
   content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|bb|bbb    Id1|Id2|Id3
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      tsv = tsv.merge({"row3" => [["A3"], ["B3"], ["Id4"]]})
      assert TSV === tsv
    end
  end
end
