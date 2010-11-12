require File.dirname(__FILE__) + '/../../test_helper'
require 'tsv'
require 'rbbt/util/tmpfile'
require 'rbbt/sources/organism'

class TestTSV < Test::Unit::TestCase
  def test_keep_empty
    content =<<-EOF
#Id ValueA ValueB Comment
row1 a|aa|aaa b c
row2 A B 
    EOF

    TmpFile.with_file(content) do |filename|
      data, key_field, fields = TSV.parse(File.open(filename), :sep => /\s+/, :keep_empty => true)
      assert_equal ["c"], data["row1"][2]
      assert_equal [""], data["row2"][2]
    end
  end

  def test_slice
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s/)
      assert_equal [["a"],["c"]], tsv.slice("ValueA", "Comment")["row1"]
    end
  end

  def _test_zipped
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row1 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      data, key_field, fields = TSV.parse(File.open(filename), :sep => /\s/, :zipped => true)
      assert_equal ["a","b","c"], data["row1"][0]
      assert_equal ["A","B","C"], data["row1"][1]
    end
  end


  def test_hash
    content =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    TmpFile.with_file(content) do |filename|
      data, key_field, fields = TSV.parse(File.open(filename), :sep => /\s+/)
      assert_equal "Id", key_field
      assert_equal ["ValueA", "ValueB"], fields
      assert_equal ["a", "aa", "aaa"], data["row1"][0]
    end
  end

  def test_tsv
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID")
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["Id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_extra
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :extra => 2)
      assert_equal ["b"], tsv["Id2"][0]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :extra => 'ValueB')
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
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :case_insensitive => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_persistence
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      tsv['Id4'] = [["row3"],["aA"],["bB","bbBB"]]
      assert_equal ["aA"], tsv["Id4"][1]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["aA"], tsv["Id4"][1]
      assert_equal [["b"],["B"]], tsv.values_at("Id1", "Id3").collect{|values| values[2]}
    end
  end

  def test_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end
  end

  def test_values_at
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => true)
      assert index.values_at(["Antoco", "row1"]).first.include? "Id1"
    end
  end

  def test_helpers
    filename = File.join(Organism.datadir('Sce'), 'identifiers')

    index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
    assert index['1020'].include? 'CDK5'
    index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
    assert index[[nil,'1020']].include? 'CDK5'
    index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
    assert index[['MISSING','1020']].include? 'CDK5'
  end
end

