require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/tsv'
require 'rbbt/util/tsv/index'

class TestTSVManipulate < Test::Unit::TestCase

  def test_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :persistence => false)
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
    end
  end

  def test_index_headerless
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      index = tsv.index(:case_insensitive => true, :target => 2)
      assert index["row1"].include? "Id1"
    end
  end


  def test_best_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
row3    A    a|B    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => false, :order => true)
      ddd index
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
    end
  end

  #{{{ Test Attach

  def ___test_smart_merge_single
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ValueC    ValueB    OtherID
c|cc|ccc    b    Id1|Id2
C    B    Id3
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1 = tsv1.smart_merge tsv2, "ValueB"

    assert_equal "C", tsv1["row2"]["ValueC"]
    assert %w(c cc ccc).include? tsv1["row1"]["ValueC"]
    assert_equal "Id1", tsv1["row1"]["OtherID"]
  end

  def test_index_to_key
     content =<<-EOF
#: :sep=/\\s+/
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content) do |filename|
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/, :key => "ValueA", :case_insensitive =>  true)
    end
  end
end

