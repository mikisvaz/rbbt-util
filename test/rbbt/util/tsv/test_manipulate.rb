require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/tsv'
require 'rbbt/util/tsv/manipulate'

class TestTSVManipulate < Test::Unit::TestCase

  def test_indentify_fields
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s/)
      assert_equal :key, tsv.identify_field("ID")
    end
  end


  def test_reorder_simple
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)

      tsv1 = tsv.reorder("ValueA")

      assert_equal "ValueA", tsv1.key_field
      assert_equal %w(Id ValueB OtherID), tsv1.fields
      assert_equal ["B"], tsv1["A"]["ValueB"]
      assert_equal ["b","C"], tsv1["a"]["ValueB"]
      assert_equal ["b"], tsv1["aa"]["ValueB"]

    end
  end

  def test_reorder_simple_headerless
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)

      tsv1 = tsv.reorder 0 

      assert_nil tsv1.key_field
      assert_equal ["B"], tsv1["A"][1]
      assert_equal ["b","C"], tsv1["a"][1]
      assert_equal ["b"], tsv1["aa"][1]
      assert_equal ["row1"], tsv1["aa"][0]
      assert_equal ["row1","row3"], tsv1["a"][0]
    end
  end


  def test_reorder_remove_field
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)

      tsv1 = tsv.reorder("ValueA", ["ValueB", "Id"])

      assert_equal "ValueA", tsv1.key_field
      assert_equal %w(ValueB Id), tsv1.fields
      assert_equal ["B"], tsv1["A"]["ValueB"]
      assert_equal ["b","C"], tsv1["a"]["ValueB"]
      assert_equal ["row1"], tsv1["aa"]["Id"]
      assert_equal ["row1","row3"], tsv1["a"]["Id"]
    end
  end

  def test_through
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)

      tsv.through "ValueA" do |key, values|
        assert(tsv.keys.include? values["Id"].first)
      end
    end
  end


  def test_slice
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s/)
      assert_equal [["a"],["c"]], tsv.reorder(:key, ["ValueA", "Comment"])["row1"]
    end
  end

  def test_sort
    content =<<-EOF
#Id    ValueA    ValueB    OtherID    Pos
row1    a|aa|aaa    b    Id1|Id2    2
row2    A    B    Id3    1
row3    A|AA|AAA|AAA    B    Id3    3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal %w(row1 row2 row3), tsv.sort
      assert_equal %w(row1 row2 row3), tsv.sort(:key)
      assert_equal %w(row2 row1 row3), tsv.sort("Pos")
      assert_equal %w(row3 row2 row1), tsv.sort("ValueA") do |values| values["ValueA"].length end
    end
  end

  def test_select
     content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename + '#:sep=/\s+/')
      assert tsv.type == :double
      
      new = tsv.select %w(b Id4)
      assert_equal %w(row1 row3).sort, new.keys

      new = tsv.select "ValueB" => %w(b Id4)
      assert_equal %w(row1).sort, new.keys

      new = tsv.select /b|Id4/
      assert_equal %w(row1 row3).sort, new.keys

      new = tsv.select "ValueB" => /b|Id4/
      assert_equal %w(row1).sort, new.keys

      tsv = TSV.new(filename + '#:sep=/\s+/#:type=:flat')
      assert tsv.type != :double
      
      new = tsv.select %w(b Id4)
      assert_equal %w(row1 row3).sort, new.keys.sort


      new = tsv.select do |k,v| v["ValueA"].include? "A" end
      assert_equal %w(row2).sort, new.keys.sort
    end
  end

  def test_process
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)

      tsv.process "ValueA" do |field_values,key,values|
        field_values.collect{|v| "Pref:#{v}"}
      end

      assert_equal ["Pref:A"], tsv["row2"]["ValueA"]
    end
  end

  def test_add_field
     content =<<-EOF
#Id    LetterValue#ValueA    LetterValue#ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF
 
    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename + '#:sep=/\s+/')
      tsv.add_field "Str length" do |k,v| 
        (v.flatten * " ").length 
      end

      assert tsv.fields.include?("Str length")
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



#  def test_smart_merge_single
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
#      tsv1 = TSV.new(File.open(filename), :list, :sep => /\s+/)
#    end
#
#    TmpFile.with_file(content2) do |filename|
#      tsv2 = TSV.new(File.open(filename), :list, :sep => /\s+/)
#    end
#
#    tsv1.smart_merge tsv2, "ValueB"
#
#    assert_equal "C", tsv1["row2"]["ValueC"]
#    assert %w(c cc ccc).include? tsv1["row1"]["ValueC"]
#    assert_equal "Id1", tsv1["row1"]["OtherID"]
#  end
#
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

