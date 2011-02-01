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
      assert_equal "OtherID", index.key_field
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
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
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.key_field
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end
  end


#  def test_open_file
#    content =<<-EOF
##Id    ValueA    ValueB    OtherID
#row1    a|aa|aaa    b    Id1|Id2
#row2    A    B    Id3
#row3    a    C    Id4
#    EOF
#
#    TmpFile.with_file(content) do |filename|
#      tsv = TSV.open_file(filename + '#:sep=/\s+/#:key=OtherID')
#      assert_equal "OtherID", tsv.key_field
#      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
#      assert_equal ["a", "aa", "aaa"], tsv["Id1"][1]
#      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
#    end
#  end
#
#
#


#  def test_helpers
#    begin
#      require 'rbbt/sources/organism'
#      filename = File.join(Organism.datadir('Sce'), 'identifiers')
#      missing = true
#      index = TSV.index(filename, :persistence => true, :key => "Associated Gene Name")
#      assert index['1020'].include? 'CDK5'
#      index = TSV.index(filename, :persistence => true, :key => "Associated Gene Name")
#      assert index[[nil,'1020']].include? 'CDK5'
#      index = TSV.index(filename, :persistence => true, :key => "Associated Gene Name")
#      assert index[['MISSING','1020']].include? 'CDK5'
#    rescue Exception
#    end
#  end
#



  def _test_smart_merge_single
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

#  def test_smart_merge
#    content1 =<<-EOF
##Id    ValueA    ValueB
#row1    a|aa|aaa    b
#row2    A    B
#    EOF
#
#    content2 =<<-EOF
##ValueC    ValueB    OtherID
#c|cc|ccc    b    Id1|Id2
#C    B    Id3
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    tsv1.smart_merge tsv2, "ValueB"
#
#    assert_equal %w(C), tsv1["row2"]["ValueC"]
#    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
#  end
#
#  def test_smart_merge_through_index_find_headers
#    content1 =<<-EOF
##Id    ValueA    ValueBB
#row1    a|aa|aaa    bb
#row2    A    BB
#    EOF
#
#    content2 =<<-EOF
##ValueC    ValueB    OtherID    ValueA
#c|cc|ccc    b    Id1|Id2    aaaa
#C    B    Id3    AA
#    EOF
#
#    index =<<-EOF
##ValueB    ValueBB
#b    bb
#B    BB
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(index) do |filename|
#      index = TSV.index(filename, :sep => /\s+/)
#    end
#
#    tsv1.smart_merge tsv2, index
#
#    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
#    assert_equal %w(C), tsv1["row2"]["ValueC"]
#
#    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
#  end
#
#
#  def test_smart_merge_through_string_find_headers
#    content1 =<<-EOF
##Id    ValueA    ValueBB
#row1    a|aa|aaa    bb
#row2    A    BB
#    EOF
#
#    content2 =<<-EOF
##ValueC    ValueB    OtherID    ValueA
#c|cc|ccc    b    Id1|Id2    aaaa
#C    B    Id3    AA
#    EOF
#
#    index =<<-EOF
##ValueB    ValueBB
#b    bb
#B    BB
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(index) do |filename|
#      tsv1.smart_merge tsv2, "through:#{filename}#:sep=/\\s+/"
#    end
#
#    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
#    assert_equal %w(C), tsv1["row2"]["ValueC"]
#
#    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
#  end
#
#  def test_smart_merge_through_string
#    content1 =<<-EOF
##Id    ValueA    ValueBB
#row1    a|aa|aaa    bb
#row2    A    BB
#    EOF
#
#    content2 =<<-EOF
##ValueC    ValueB    OtherID    ValueA
#c|cc|ccc    b    Id1|Id2    aaaa
#C    B    Id3    AA
#    EOF
#
#    index =<<-EOF
##ValueB    ValueBB
#b    bb
#B    BB
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(index) do |filename|
#      tsv1.smart_merge tsv2, "through:#{filename}#:sep=/\\s+/#using:ValueBB"
#    end
#
#    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
#    assert_equal %w(C), tsv1["row2"]["ValueC"]
#
#    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
#  end
#
#  def test_smart_merge_common_fields
#    content1 =<<-EOF
##Id    ValueA    ValueB
#row1    a|aa|aaa    b
#row2    A    B
#    EOF
#
#    content2 =<<-EOF
##ValueC    ValueB    OtherID    ValueA
#c|cc|ccc    b    Id1|Id2    aaaa
#C    B    Id3    AA
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    tsv1.smart_merge tsv2, "ValueB"
#
#    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
#    assert_equal %w(C), tsv1["row2"]["ValueC"]
#
#    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
#  end
#
#  def test_smart_merge_headerless
#    content1 =<<-EOF
#row1    a|aa|aaa    b
#row2    A    B
#    EOF
#
#    content2 =<<-EOF
#c|cc|ccc    b    Id1|Id2
#C    B    Id3
#    EOF
#
#    tsv1 = tsv2 = nil
#    TmpFile.with_file(content1) do |filename|
#      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
#    end
#
#    tsv1.smart_merge tsv2, 1
#
#    assert_equal %w(C), tsv1["row2"][2]
#    assert_equal %w(Id1 Id2), tsv1["row1"][3]
#  end


#  def test_join
#     content =<<-EOF
##Id    LetterValue#ValueA    LetterValue#ValueB    OtherID
#row1    a|aa|aaa    b    Id1|Id2
#row2    A    B    Id3
#row3    a    C    Id4
#    EOF
# 
#    TmpFile.with_file(content) do |filename|
#      tsvs = TSV.headers(filename, :sep => /\s+/)[1..-1].collect do |field|
#        tsv = TSV.new filename, :others => field, :sep => /\s+/
#        tsv
#      end
#      final = TSV.join *tsvs
#      ddd final
#    end
#  end
#
#
end

