require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/tsv/parse'

class TestTSVParse < Test::Unit::TestCase

  def test_keep_empty
    content =<<-EOF
#Id ValueA ValueB Comment
row1 a|aa|aaa b c
row2 A B
    EOF

    TmpFile.with_file(content) do |filename|
      data = {}
      data, extra = TSV.parse(File.open(filename), :sep => /\s+/, :keep_empty => true)
      assert_equal ["ValueA", "ValueB", "Comment"], extra[:fields]
      assert_equal ["c"], data["row1"][2]
      assert_equal [""], data["row2"][2]
    end
  end

  def test_break_with_fix
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.parse(File.open(filename), :sep => /\s+/, :fix => proc{|l| l =~ /^row2/? nil : l})
      assert_equal %w(row1), tsv.first.keys
    end
  end

  def test_hash
    content =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    TmpFile.with_file(content) do |filename|
      data = {}
      data, extra = TSV.parse(File.open(filename), :sep => /\s+/)
      assert_equal "Id", extra[:key_field]
      assert_equal ["ValueA", "ValueB"], extra[:fields]
      assert_equal ["a", "aa", "aaa"], data["row1"][0]
    end
  end

end

