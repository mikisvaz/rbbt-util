require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/stream'
require 'rbbt'

class TestStream < Test::Unit::TestCase
  def test_collapse_stream
    text=<<-EOF
#: :sep=" "
#Row LabelA LabelB LabelC
row1 A B C
row1 a b c
row2 AA BB CC
row2 aa bb cc
    EOF

    s = StringIO.new text
    tsv = TSV.open TSV.collapse_stream(s)
    assert_equal ["A", "a"], tsv["row1"][0]
    assert_equal ["BB", "bb"], tsv["row2"][1]
  end

  def test_paste_stream
    text1=<<-EOF
#: :sep=" "
#Row LabelA LabelB LabelC
row1 A B C
row2 AA BB CC
row3 AAA BBB CCC
    EOF

    text2=<<-EOF
#: :sep=" "
#Row Labela Labelb 
row1 a b
row2 aa bb
row3 aaa bbb
    EOF

    text3=<<-EOF
#: :sep=" "
#Row LabelC
row1 c
row2 cc
row3 ccc
    EOF

    s1 = StringIO.new text1
    s2 = StringIO.new text2
    s3 = StringIO.new text3
    tsv = TSV.open TSV.paste_streams([s1,s2,s3], :sep => " ", :type => :list)
    assert_equal ["A", "B", "C", "a", "b", "c"], tsv["row1"]
    assert_equal ["AA", "BB", "CC", "aa", "bb", "cc"], tsv["row2"]
    assert_equal ["AAA", "BBB", "CCC", "aaa", "bbb", "ccc"], tsv["row3"]
  end

  def test_paste_stream_sort
    text1=<<-EOF
#: :sep=" "
#Row LabelA LabelB LabelC
row2 AA BB CC
row1 A B C
row3 AAA BBB CCC
    EOF

    text2=<<-EOF
#: :sep=" "
#Row Labela Labelb 
row1 a b
row3 aaa bbb
row2 aa bb
    EOF

    text3=<<-EOF
#: :sep=" "
#Row Labelc
row3 ccc
row1 c
row2 cc
    EOF

    s1 = StringIO.new text1
    s2 = StringIO.new text2
    s3 = StringIO.new text3
    tsv = TSV.open TSV.paste_streams([s1,s2,s3], :sep => " ", :type => :list, :sort => true)
    assert_equal "Row", tsv.key_field
    assert_equal %w(LabelA LabelB LabelC Labela Labelb Labelc), tsv.fields
    assert_equal ["A", "B", "C", "a", "b", "c"], tsv["row1"]
    assert_equal ["AA", "BB", "CC", "aa", "bb", "cc"], tsv["row2"]
    assert_equal ["AAA", "BBB", "CCC", "aaa", "bbb", "ccc"], tsv["row3"]
  end

  def test_paste_stream_missing
    text1=<<-EOF
#: :sep=" "
#Row LabelA LabelB LabelC
row2 AA BB CC
row1 A B C
    EOF

    text2=<<-EOF
#: :sep=" "
#Row Labela Labelb 
row1 a b
row2 aa bb
    EOF

    text3=<<-EOF
#: :sep=" "
#Row Labelc
row3 ccc
row2 cc
    EOF

    s1 = StringIO.new text1
    s2 = StringIO.new text2
    s3 = StringIO.new text3
    tsv = TSV.open TSV.paste_streams([s1,s2,s3], :sep => " ", :type => :list, :sort => true)
    assert_equal "Row", tsv.key_field
    assert_equal %w(LabelA LabelB LabelC Labela Labelb Labelc), tsv.fields
    assert_equal ["A", "B", "C", "a", "b", ""], tsv["row1"]
    assert_equal ["AA", "BB", "CC", "aa", "bb", "cc"], tsv["row2"]
    assert_equal ["", "", "", "", "", "ccc"], tsv["row3"]
  end
end
