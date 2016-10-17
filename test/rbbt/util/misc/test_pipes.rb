require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'test/unit'
require 'rbbt/tsv'
require 'rbbt/util/misc'

class TestMiscPipes < Test::Unit::TestCase

  def test_collapse_stream
    text=<<-EOF
row1 A B C
row1 a b c
row2 AA BB CC
row2 aa bb cc
    EOF

    s = StringIO.new text
    tsv = TSV.open Misc.collapse_stream(s,nil, " "), :sep => " " 
    assert_equal ["A", "a"], tsv["row1"][0]
    assert_equal ["BB", "bb"], tsv["row2"][1]
  end

  def test_collapse_sum
    text=<<-EOF
row1 12
row1 4
row2 10
row2 6
    EOF

    s = StringIO.new text
    stream = Misc.collapse_stream(s,nil, " ") do |parts|
      next nil if parts.empty?
      parts.first.split("|").collect{|p| p.to_f}.inject(0){|acc,e| acc += e}.to_s
    end
    tsv = TSV.open  stream, :sep => " " 
    ppp tsv.to_s
  end

  def test_collapse_stream_gap
    text=<<-EOF
row2 AA BB 
row2 aa bb cc
    EOF

    s = StringIO.new text
    assert Misc.collapse_stream(s, nil, " ").read =~  /\|cc$/
    
    text=<<-EOF
row2 aa bb cc
row2 AA BB 
    EOF

    s = StringIO.new text
    assert Misc.collapse_stream(s, nil, " ").read =~  /cc\|$/

    text=<<-EOF
row2 AA BB
row2 aa bb cc
    EOF

    s = StringIO.new text
    assert Misc.collapse_stream(s, nil, " ").read =~  /\|cc$/

  end


  def test_paste_stream
    text1=<<-EOF
row1 A B C
row2 AA BB CC
row3 AAA BBB CCC
    EOF

    text2=<<-EOF
row1 a b
row2 aa bb
    EOF

    text3=<<-EOF
row1 c
row2 cc
row3 ccc
    EOF

    s1 = StringIO.new text1
    s2 = StringIO.new text2
    s3 = StringIO.new text3
    tsv = TSV.open Misc.paste_streams([s1,s2,s3], nil, " "), :sep => " ", :type => :list
    assert_equal ["A", "B", "C", "a", "b", "c"], tsv["row1"]
    assert_equal ["AA", "BB", "CC", "aa", "bb", "cc"], tsv["row2"]
    assert_equal ["AAA", "BBB", "CCC", "", "", "ccc"], tsv["row3"]
  end

  def test_sort_stream
    text =<<-EOF
##
##
##
#Row LabelA LabelB LabelC
row2 AA BB CC
row3 AAA BBB CCC
row1 A B C
    EOF
    s = StringIO.new text
    sorted = Misc.sort_stream(s)
    assert_equal %w(## ## ## #Row row2 row3 row1), text.split("\n").collect{|l| l.split(" ").first}
    assert_equal %w(## ## ## #Row row1 row2 row3), sorted.read.split("\n").collect{|l| l.split(" ").first}
  end

  def test_sort_stream2
    text =<<-EOF
##
##
##
#Row LabelA LabelB LabelC
row2 AA BB CC
row3 AAA BBB CCC
row1 A B C
    EOF
    s = StringIO.new text
    sorted = Misc.sort_stream(Misc.sort_stream(s))
    assert_equal %w(## ## ## #Row row2 row3 row1), text.split("\n").collect{|l| l.split(" ").first}
    assert_equal %w(## ## ## #Row row1 row2 row3), sorted.read.split("\n").collect{|l| l.split(" ").first}
  end

  def test_dup_stream
    text =<<-EOF
#: :sep=" "
#Row LabelA LabelB LabelC
row2 AA BB CC
row3 AAA BBB CCC
row1 A B C
    EOF

    text = text * 10000
    TmpFile.with_file(text) do |tmp|
      io = Open.open(tmp)
      dup = Misc.dup_stream(io)
      Misc.consume_stream io, true
      assert_equal text, dup.read
    end


    TmpFile.with_file(text) do |tmp|
      io = Open.open(tmp)
      dup = Misc.dup_stream(io)
      Misc.consume_stream dup, true
      assert_equal text, io.read
    end
  end

  def test_dup_stream_multiple
    text =<<-EOF
row2 AA BB CC
row3 AAA BBB CCC
row1 A B C
    EOF
    Log.severity = 0

    text = text * 10000
    num = 5
    str_io = StringIO.new
    strs = []
    num.times{ strs << StringIO.new }
    TmpFile.with_file(text) do |tmp|
      io = Open.open(tmp)
      copies = Misc.dup_stream_multiple(io, num)


      copies.each_with_index do |cio,i|
        cio.add_callback do 
          str = strs[i] 
          str.rewind
          assert_equal text, str.read
        end
        Misc.consume_stream cio, true, strs[i], false
      end


      Misc.consume_stream io, false, str_io, false
      str_io.rewind
      assert_equal text, str_io.read

    end
  end

  def test_remove_lines
    text1 =<<-EOF
line1
line2
line3
line4
    EOF
    text2 =<<-EOF
line3
line1
    EOF

    Log.severity = 0
    TmpFile.with_file(text1) do |file1|
      TmpFile.with_file(text2) do |file2|
        assert ! Misc.remove_lines(file1, file2, true).read.split("\n").include?("line1")
        assert Misc.remove_lines(file1, file2, true).read.split("\n").include?("line2")
      end
    end
  end



  def test_select_lines
    text1 =<<-EOF
line1
line2
line3
line4
    EOF
    text2 =<<-EOF
line3
line1
line5
    EOF

    Log.severity = 0
    TmpFile.with_file(text1) do |file1|
      TmpFile.with_file(text2) do |file2|
        assert Misc.select_lines(file1, file2, true).read.split("\n").include?("line1")
        assert ! Misc.select_lines(file1, file2, true).read.split("\n").include?("line2")
      end
    end
  end

  def test_consume_into_string_io
    text =<<-EOF
line1
line2
line3
line4
    EOF

    TmpFile.with_file(text) do |file|
      out = StringIO.new
      io = Open.open(file)
      Misc.consume_stream(io, false, out, false)
      out.rewind
      assert_equal text, out.read
    end


  end
end
