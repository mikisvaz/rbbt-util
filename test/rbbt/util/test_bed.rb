require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/bed'

class TestBed < Test::Unit::TestCase
  def load_data(data)
    Log.debug("Data:\n#{Open.read(data)}")
    tsv = TSV.new(data, :sep=>":", :cast => proc{|e| e =~ /(\s*)(_*)/; ($1.length..($1.length + $2.length - 1))}, :unique => true)
    tsv.add_field "Start" do |key, values|
      values["Range"].first
    end
    tsv.add_field "End" do |key, values|
      values["Range"].last
    end
 
    tsv
  end

  def test_point
    data =<<-EOF
#ID Pos
a 1
b 10
c 20
d 12
e 26
f 11
g 25
    EOF

    TmpFile.with_file(data) do |f|
      bed = Bed.new TSV.new(f, :sep=>" ", :unique => true), :key => "Pos" , :value => "ID"

      assert_equal %w(), bed[0].sort
      assert_equal %w(b), bed[10].sort
      assert_equal %w(a b c d f), bed[(0..20)].sort
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

    TmpFile.with_file(data) do |f|
      bed = Bed.new load_data(f), :range => ["Start" , "End"], :value => "ID"

      assert_equal %w(), bed[0].sort
      assert_equal %w(b), bed[1].sort
      assert_equal %w(), bed[20].sort
      assert_equal %w(), bed[(20..100)].sort
      assert_equal %w(a b d), bed[3].sort
      assert_equal %w(a b c d e), bed[(3..4)].sort
    end
  end

  def test_format
    entry = Bed::Entry.new "Hola", 0, 10, nil
    format = Bed::FixWidthTable.format(entry, 100)
    assert_equal entry, Bed::FixWidthTable.unformat(format, 100)
  end

  def test_table
    TmpFile.with_file do |f|
      table = Bed::FixWidthTable.new(f, 100, false)
      table.add Bed::Entry.new "Entry1", 0, 10, nil
      table.add Bed::Entry.new "Entry2", 10, 20, nil
      table.add Bed::Entry.new "Entry3", 10, 20, nil

      table.read 

      assert_equal "Entry2", table[1].value
    end

  end

  def test_range_persistence
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

    TmpFile.with_file(data) do |f|
      bed = Bed.new load_data(f), :range => ["Start" , "End"], :value => "ID", :persistence => true

      bed = Bed.new load_data(f), :range => ["Start" , "End"], :value => "ID", :persistence => true
      
      assert_equal %w(), bed[0].sort
      assert_equal %w(b), bed[1].sort
      assert_equal %w(), bed[20].sort
      assert_equal %w(), bed[(20..100)].sort
      assert_equal %w(a b d), bed[3].sort
      assert_equal %w(a b c d e), bed[(3..4)].sort
    end
  end
end
