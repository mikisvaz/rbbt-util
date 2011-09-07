require File.join(File.expand_path(File.dirname(__FILE__)), '../', 'test_helper.rb')
require 'rbbt/fix_width_table'
require 'rbbt/tsv'
require 'rbbt/tsv/manipulate'

class TestFixWidthTable < Test::Unit::TestCase
  def load_data(data)
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

  def test_options
    TmpFile.with_file do |filename|
      f = FixWidthTable.new filename, 100, true
      f.close

      f1 = FixWidthTable.new filename, 100, false

      assert_equal true, f1.range
    end
  end

  def test_add
    TmpFile.with_file do |filename|
      f = FixWidthTable.new filename, 100, true
      f.add [1,2,0], "test1"
      f.add [3,4,0], "test2"
      f.read

      assert_equal 1, f.pos(0)
      assert_equal 3, f.pos(1)
      assert_equal 2, f.pos_end(0)
      assert_equal 4, f.pos_end(1)
      assert_equal 0, f.overlap(0)
      assert_equal 0, f.overlap(1)
      assert_equal "test1", f.value(0)
      assert_equal "test2", f.value(1)
    end
 
  end

  def test_point
    data =<<-EOF
#: :sep=/\\s+/#:type=:single#:cast=:to_i
#ID Pos
a 1
b 10
c 20
d 12
e 26
f 11
g 25
    EOF
    TmpFile.with_file(data) do |datafile|
      tsv = TSV.open datafile
      TmpFile.with_file do |filename|
        f = FixWidthTable.new filename, 100, false
        f.add_point tsv
        f.read

        assert_equal %w(), f[0].sort
        assert_equal %w(b), f[10].sort
        assert_equal %w(a b c d f), f[(0..20)].sort
      end
    end
  end

  def test_range
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
      TmpFile.with_file do |filename|
        f = FixWidthTable.new filename, 100, true
        f.add_range tsv
        f.read

        assert_equal %w(), f[0].sort
        assert_equal %w(b), f[1].sort
        assert_equal %w(), f[20].sort
        assert_equal %w(), f[(20..100)].sort
        assert_equal %w(a b d), f[3].sort
        assert_equal %w(a b c d e), f[(3..4)].sort
      end
    end
  end
end

