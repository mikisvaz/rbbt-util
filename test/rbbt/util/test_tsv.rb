require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'

class TestTSV < Test::Unit::TestCase
  def test_keep_empty
    content =<<-EOF
#Id ValueA ValueB Comment
row1 a|aa|aaa b c
row2 A B
    EOF

    TmpFile.with_file(content) do |filename|
      data = {}
      key_field, fields = TSV.parse(data, File.open(filename), :sep => /\s+/, :keep_empty => true)
      assert_equal ["ValueA", "ValueB", "Comment"], fields
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
      assert_equal [["a"],["c"]], tsv.reorder(:main, ["ValueA", "Comment"])["row1"]
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

  def test_hash
    content =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    TmpFile.with_file(content) do |filename|
      data = {}
      key_field, fields = TSV.parse(data, File.open(filename), :sep => /\s+/)
      assert_equal "Id", key_field
      assert_equal ["ValueA", "ValueB"], fields
      assert_equal ["a", "aa", "aaa"], data["row1"][0]
    end
  end

  def test_large
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :large => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
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

  def test_open_file
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open_file(filename + '#:sep=/\s+/#:native=OtherID')
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
      tsv = TSV.new(filename, :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      tsv['Id4'] = [["row3"],["aA"],["bB","bbBB"]]
      assert_equal ["aA"], tsv["Id4"][1]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["aA"], tsv["Id4"][1]
      assert_equal [["b"],["B"]], tsv.values_at("Id1", "Id3").collect{|values| values[2]}
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true, :flatten => true)
      assert(tsv["Id3"].include? "A")
    end
  end

  def test_index_headerless
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      index = tsv.index(:case_insensitive => true, :field => 2)
      assert index["row1"].include? "Id1"
    end
  end


  def test_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => false)
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

  def test_best_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => false, :order => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
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
      assert index.values_at(*["row1"]).first.include? "Id1"
    end
  end

  def test_named_array
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
      assert_equal ["a", "aa", "aaa"], tsv["Id2"]["ValueA"]

      tsv_sliced = tsv.reorder(:main, ["ValueA", "ValueB"])

      assert_equal ["ValueA", "ValueB"], tsv_sliced.fields
      assert_equal ["a", "aa", "aaa"], tsv_sliced["id1"][0]
      assert_equal ["a", "aa", "aaa"], tsv_sliced["Id2"]["ValueA"]
    end
  end

  def test_helpers
    begin
      require 'rbbt/sources/organism'
      filename = File.join(Organism.datadir('Sce'), 'identifiers')
      missing = true
      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index['1020'].include? 'CDK5'
      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index[[nil,'1020']].include? 'CDK5'
      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index[['MISSING','1020']].include? 'CDK5'
    rescue Exception
    end
  end


  def test_sort
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal "row2", tsv.sort{|a,b| a[1]["ValueB"] <=> b[1]["ValueA"] }.first[0]
      assert_equal "B", tsv.sort{|a,b| a[1]["ValueB"] <=> b[1]["ValueA"] }.first[1]["ValueB"].first
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal "row2", tsv.sort_by{|k,v| v["ValueB"]}.first[0]
      assert_equal "B", tsv.sort_by{|k,v| v["ValueB"]}.first[1]["ValueB"].first
    end
  end

  def test_to_s
    content =<<-EOF
#Id	ValueA	ValueB	OtherID
row1	a|aa|aaa	b	Id1|Id2
row2	A	B	Id3
    EOF
    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal content, tsv.to_s
    end
  end


  def test_smart_merge_single
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
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/, :unique => true)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/, :unique => true)
    end

    tsv1.smart_merge tsv2, "ValueB"

    assert_equal "C", tsv1["row2"]["ValueC"]
    assert %w(c cc ccc).include? tsv1["row1"]["ValueC"]
    assert_equal "Id1", tsv1["row1"]["OtherID"]
  end

  def test_smart_merge
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
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    tsv1.smart_merge tsv2, "ValueB"

    assert_equal %w(C), tsv1["row2"]["ValueC"]
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
  end

  def test_smart_merge_through_index_find_headers
    content1 =<<-EOF
#Id    ValueA    ValueBB
row1    a|aa|aaa    bb
row2    A    BB
    EOF

    content2 =<<-EOF
#ValueC    ValueB    OtherID    ValueA
c|cc|ccc    b    Id1|Id2    aaaa
C    B    Id3    AA
    EOF

    index =<<-EOF
#ValueB    ValueBB
b    bb
B    BB
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(index) do |filename|
      index = TSV.index(filename, :sep => /\s+/)
    end

    tsv1.smart_merge tsv2, index

    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
    assert_equal %w(C), tsv1["row2"]["ValueC"]

    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
  end


  def test_smart_merge_through_string_find_headers
    content1 =<<-EOF
#Id    ValueA    ValueBB
row1    a|aa|aaa    bb
row2    A    BB
    EOF

    content2 =<<-EOF
#ValueC    ValueB    OtherID    ValueA
c|cc|ccc    b    Id1|Id2    aaaa
C    B    Id3    AA
    EOF

    index =<<-EOF
#ValueB    ValueBB
b    bb
B    BB
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(index) do |filename|
      tsv1.smart_merge tsv2, "through:#{filename}#:sep=/\\s+/"
    end

    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
    assert_equal %w(C), tsv1["row2"]["ValueC"]

    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
  end

  def test_smart_merge_through_string
    content1 =<<-EOF
#Id    ValueA    ValueBB
row1    a|aa|aaa    bb
row2    A    BB
    EOF

    content2 =<<-EOF
#ValueC    ValueB    OtherID    ValueA
c|cc|ccc    b    Id1|Id2    aaaa
C    B    Id3    AA
    EOF

    index =<<-EOF
#ValueB    ValueBB
b    bb
B    BB
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(index) do |filename|
      tsv1.smart_merge tsv2, "through:#{filename}#:sep=/\\s+/#using:ValueBB"
    end

    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
    assert_equal %w(C), tsv1["row2"]["ValueC"]

    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
  end
  def test_smart_merge_common_fields
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ValueC    ValueB    OtherID    ValueA
c|cc|ccc    b    Id1|Id2    aaaa
C    B    Id3    AA
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    tsv1.smart_merge tsv2, "ValueB"

    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
    assert_equal %w(C), tsv1["row2"]["ValueC"]

    assert_equal %w(a aa aaa aaaa), tsv1["row1"]["ValueA"]
  end

  def test_smart_merge_headerless
    content1 =<<-EOF
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
c|cc|ccc    b    Id1|Id2
C    B    Id3
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :sep => /\s+/)
    end

    tsv1.smart_merge tsv2, 1

    assert_equal %w(C), tsv1["row2"][2]
    assert_equal %w(Id1 Id2), tsv1["row1"][3]
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

      tsv1 = tsv.reorder(0)

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
end

