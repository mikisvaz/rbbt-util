require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'

class TestTSV < Test::Unit::TestCase

  def test_tsv
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s+/, :key => "OtherID")
      assert_equal :double, tsv.type
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["Id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_grep
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :grep => %w(row1))
      assert tsv.keys.include? "row1"
      assert( ! tsv.keys.include?("row2"))
    end
  end

  def test_open_stringoptions
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename + '#:sep=/\s+/')
      assert_equal ["A"], tsv["row2"]["ValueA"]
    end
  end

  def test_headers
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      assert_equal ['ID', 'ValueA', 'ValueB', 'Comment'], TSV.headers(filename, :sep => ' ')
      assert_equal nil, TSV.headers(filename, :sep => ' ', :header_hash => "##")
    end
  end

  def test_headerless
    content =<<-EOF
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      assert_equal 3, TSV.new(filename, :sep => ' ')['row1'].length
    end
  end

  def test_extra
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :others => 2)
      assert_equal ["b"], tsv["Id2"][0]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :others => 'ValueB')
      assert_equal ["b"], tsv["Id2"][0]
    end
  end

  def test_case
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :case_insensitive => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]

      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :case_insensitive => false)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_nil tsv["id1"]
    end
  end

  def test_persistence
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :sep => /\s+/, :key => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      tsv.write
      tsv['Id4'] = [["row3"],["aA"],["bB","bbBB"]]
      assert_equal ["aA"], tsv["Id4"][1]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields

      assert_equal ["aA"], tsv["Id4"][1]
      assert_equal [["b"],["B"]], tsv.values_at("Id1", "Id3").collect{|values| values[2]}
      tsv = TSV.new(File.open(filename), :flat, :sep => /\s+/, :key => "OtherID", :persistence => false)
      assert(tsv["Id3"].include? "A")
    end
  end

  def test_named_array
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :case_insensitive => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"]["ValueA"]
   end
  end


end

