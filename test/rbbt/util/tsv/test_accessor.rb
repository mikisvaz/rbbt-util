require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/tsv/accessor'
require 'rbbt/util/tsv'

class TestTSVAcessor < Test::Unit::TestCase

  def test_zip_fields
    a = [%w(1 2), %w(a b)]
    assert_equal a, TSV.zip_fields(TSV.zip_fields(a))
  end

  def test_values_at
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :list, :sep => /\s+/, :key => "OtherID", :persistence => true)
      assert_equal "row2", tsv.values_at("Id1", "Id3").last.first 
    end
  end

  def test_to_s
    content =<<-EOF
#Id	ValueA	ValueB	OtherID
row1	a|aa|aaa	b	Id1|Id2
row2	A	B	Id3
    EOF

    content2 =<<-EOF
#Id	ValueA	ValueB	OtherID
row1	a|aa|aaa	b	Id1|Id2
row2	A	B	Id3
    EOF
 
    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal content, tsv.to_s.sub(/^#: [^\n]*\n/s,'')
    end
  end

  def test_to_s_ordered
    content =<<-EOF
#Id	ValueA	ValueB	OtherID
row1	a|aa|aaa	b	Id1|Id2
row2	A	B	Id3
    EOF

    content2 =<<-EOF
#Id	ValueA	ValueB	OtherID
row2	A	B	Id3
row1	a|aa|aaa	b	Id1|Id2
    EOF


    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal content, tsv.to_s(%w(row1 row2)).sub(/^#: [^\n]*\n/s,'')
      assert_not_equal content, tsv.to_s(%w(row2 row1)).sub(/^#: [^\n]*\n/s,'')
      assert_equal content2, tsv.to_s(%w(row2 row1)).sub(/^#: [^\n]*\n/s,'')
    end
  end

  def test_field_compare
     content =<<-EOF
#Id    Letter:LetterValue    Other:LetterValue    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    a    C    Id4
    EOF
    
    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename + '#:sep=/\s+/')

      assert tsv.fields.include?("LetterValue")
    end
  end

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

  def test_named_fields
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s/)

      assert_equal "ValueA", tsv.fields["ValueA"]
    end
  end

  def test_namespace
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s/, :namespace => "TEST")

      assert_equal "TEST", tsv.fields.first.namespace
      assert_equal "TEST", tsv.key_field.namespace
    end
  end

  def test_delete
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :double, :sep => /\s/)
      tsv.delete "row1"
      
      assert_equal 1, tsv.size
      assert_equal ["row2"], tsv.keys

      tsv = TSV.new(File.open(filename), :double, :sep => /\s/, :persistence => true)
      assert_equal 2, tsv.size
      assert_equal ["row1", "row2"], tsv.keys
      tsv.write
      tsv.delete "row1"
      tsv.read

      assert_equal 1, tsv.size
      assert_equal ["row2"], tsv.keys
    end
  end


end

