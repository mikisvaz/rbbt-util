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
    tsv = TSV.open Misc.paste_streams([s1,s2,s3],nil, " "), :sep => " ", :type => :list
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
end
