require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/index'

class TestTSVManipulate < Test::Unit::TestCase

  def load_segment_data(data)
    tsv = TSV.open(data, :list, :sep=>":", :cast => proc{|e| e =~ /(\s*)(_*)/; ($1.length..($1.length + $2.length - 1))})
    tsv.add_field "Start" do |key, values|
      values["Range"].first
    end
    tsv.add_field "End" do |key, values|
      values["Range"].last
    end

    tsv = tsv.slice ["Start", "End"]
 
    tsv
  end

  def test_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :sep => /\s+/, :key_field => "OtherID", :persistence => false)
      index = tsv.index(:case_insensitive => true, :persistence => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
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
      tsv = TSV.open(File.open(filename), :sep => /\s+/, :key_field => "OtherID")
      index = tsv.index(:order => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first
      
      index = tsv.index(:order => false)
      assert_equal "Id1", index['a'].first
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :sep => /\s+/, :key_field => "OtherID")
      index = tsv.index
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.fields.first
    end
  end


  def test_index_from_persit
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
row3    A    a|B    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(Open.open(filename), :sep => /\s+/, :key_field => "OtherID", :persist => true)
      index = tsv.index(:order => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first
    end
  end

  def test_index_to_persist
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
row3    A    a|B    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(Open.open(filename), :sep => /\s+/, :key_field => "OtherID", :persist => true)

      index = tsv.index(:order => true, :persist => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first

      tsv.delete "Id1"

      index = tsv.index(:order => true, :persist => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first

      index = tsv.index(:order => true, :persist => false)
      assert_equal "Id3", index['a'].first
    end
  end

  def test_index_static
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
row3    A    a|B    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      index = TSV.index(filename, :target => "OtherID", :data_sep => /\s+/, :order => true, :persist => false)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first
    end
  end

  def test_index_static_persist
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
row3    A    a|B    Id4
    EOF

    TmpFile.with_file(content) do |filename|
      index = TSV.index(filename, :target => "OtherID", :data_sep => /\s+/, :order => true, :persist => false)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first

      index = TSV.index(filename, :target => "OtherID", :data_sep => /\s+/, :order => true, :persist => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first

      Open.write(filename, Open.read(filename).sub(/row1.*Id1\n/,''))

      index = TSV.index(filename, :target => "OtherID", :data_sep => /\s+/, :order => true, :persist => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.fields.first
      assert index.include?('aaa')

      index = TSV.index(filename, :target => "OtherID", :data_sep => /\s+/, :order => true, :persist => false)
      assert ! index.include?('aaa')
    end
  end

  def test_pos_index
    content =<<-EOF
#Id	ValueA    ValueB    Pos
row1    a|aa|aaa    b    0|10
row2    A    B    30
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :double, :sep => /\s+/)
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
      tsv = TSV.open(File.open(filename), :double, :sep => /\s+/)
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
      tsv = load_segment_data(datafile)
      f   = tsv.range_index("Start", "End", :persist => true)

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
      TmpFile.with_file(load_segment_data(datafile)) do |tsvfile|
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


#  #{{{ Test Attach
#  def test_index_headerless
#    content =<<-EOF
#row1    a|aa|aaa    b    Id1|Id2
#row2    A    B    Id3
#    EOF
#
#    TmpFile.with_file(content) do |filename|
#      tsv = TSV.open(File.open(filename), :sep => /\s+/)
#      index = tsv.index(:target => 2)
#      assert index["row1"].include? "Id1"
#    end
#  end



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
      TmpFile.with_file(load_segment_data(datafile)) do |tsvfile|
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

