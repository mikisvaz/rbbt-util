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
      index = tsv.index(:case_insensitive => true, :persistence => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
    end

#    TmpFile.with_file(content) do |filename|
#      tsv = TSV.new(File.open(filename), :sep => /\s+/, :key => "OtherID")
#      index = tsv.index(:case_insensitive => true)
#      assert index["row1"].include? "Id1"
#      assert_equal "OtherID", index.fields.first
#    end
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
      index = tsv.index(:case_insensitive => false, :order => true, :persistence => true)
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

  def test_smart_merge_single
    content1 =<<-EOF
#: :case_insensitive=false
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#: :case_insensitive=false
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

    assert_equal "C", tsv1["row2"]["ValueC"].first
    assert %w(c cc ccc).include? tsv1["row1"]["ValueC"].first
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"].sort
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

  # {{{ Test sorted index

  def load_data(data)
    Log.debug("Data:\n#{Open.read(data)}")
    tsv = TSV.new(data, :list, :sep=>":", :cast => proc{|e| e =~ /(\s*)(_*)/; ($1.length..($1.length + $2.length - 1))})
    tsv.add_field "Start" do |key, values|
      values["Range"].first
    end
    tsv.add_field "End" do |key, values|
      values["Range"].last
    end

    tsv = tsv.slice ["Start", "End"]
 
    tsv
  end

  def test_pos_index
    content =<<-EOF
#Id	ValueA    ValueB    Pos
row1    a|aa|aaa    b    0|10
row2    A    B    30
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s+/)
      index = tsv.pos_index("Pos")
      assert_equal ["row1"], index[10]
    end
  end


  def test_range_index
    content =<<-EOF
#Id	ValueA    ValueB    Pos1    Pos2
row1    a|aa|aaa    b    0|10    10|30
row2    A    B    30   35
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s+/)
      index = tsv.pos_index("Pos1")
      assert_equal ["row1"], index[10]

      index = tsv.range_index("Pos1", "Pos2")
      assert_equal ["row1"], index[20]
    end
  end

  def test_range_index2
    data =<<-EOF
#ID:Range
#:012345678901234567890
a:   ______
b: ______
c:    _______
d:  ____
e:    ______
f:             ___
g:         ____
    EOF
    TmpFile.with_file(data) do |datafile|
      tsv = load_data(datafile)
      f   = tsv.range_index("Start", "End")

      assert_equal %w(), f[0].sort
      assert_equal %w(b), f[1].sort
      assert_equal %w(), f[20].sort
      assert_equal %w(), f[(20..100)].sort
      assert_equal %w(a b d), f[3].sort
      assert_equal %w(a b c d e), f[(3..4)].sort
    end
  end

  def test_range_index_persistent
    data =<<-EOF
#ID:Range
#:012345678901234567890
a:   ______
b: ______
c:    _______
d:  ____
e:    ______
f:             ___
g:         ____
    EOF
    TmpFile.with_file(data) do |datafile|
      TmpFile.with_file(load_data(datafile)) do |tsvfile|
        f = TSV.range_index(tsvfile, "Start", "End", :persistence => true)

        assert_equal %w(), f[0].sort
        assert_equal %w(b), f[1].sort
        assert_equal %w(), f[20].sort
        assert_equal %w(), f[(20..100)].sort
        assert_equal %w(a b d), f[3].sort
        assert_equal %w(a b c d e), f[(3..4)].sort
      end
    end
  end

  def test_range_index_persistent_with_filter
    data =<<-EOF
#ID:Range
#:012345678901234567890
a:   ______
b: ______
c:    _______
d:  ____
e:    ______
f:             ___
g:         ____
    EOF
    TmpFile.with_file(data) do |datafile|
      TmpFile.with_file(load_data(datafile)) do |tsvfile|
        f = TSV.range_index(tsvfile, "Start", "End", :filters => [["field:Start", "3"]])

        assert_equal %w(), f[0].sort
        assert_equal %w(), f[1].sort
        assert_equal %w(), f[20].sort
        assert_equal %w(), f[(20..100)].sort
        assert_equal %w(a), f[3].sort
        assert_equal %w(a), f[(3..4)].sort
      end
    end
  end

end

