require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/tsv'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestTSVAccessor < Test::Unit::TestCase

  def test_to_hash
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      hash = tsv.to_hash
      assert hash.methods.select{|m| m =~ /#{ TSV::KEY_PREFIX }/}.empty?
    end
  end

  def test_zip_new
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|bb|bbb    Id1|Id2|Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      tsv.zip_new("row1", %w(aaaa bbbb Id4))
      assert_equal %w(b bb bbb bbbb), tsv["row1"]["ValueB"]
    end
  end

  def test_tsv
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal 2, tsv.keys.length
      assert_equal 2, tsv.values.length
      assert_equal 2, tsv.collect.length
    end
  end

  def test_named_values
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal ["A"], tsv["row2"]["ValueA"]
    end
  end

  def test_to_s
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert tsv.to_s =~ /row1\ta|aa|aaa/
      assert tsv.to_s =~ /:type=:double/
    end
  end
  
  def test_entries
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal filename, tsv.options[:filename]
    end
 
  end

  def test_marshal
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :persist_serializer => :marshal, :persist => true)
      assert_equal 2, tsv.size
      assert_equal filename, tsv.options[:filename]
    end
  end

  def test_zip_fields
    a = [%w(1 2), %w(a b)]
    assert_equal a, TSV.zip_fields(TSV.zip_fields(a))
  end

  def test_indentify_fields
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :double, :sep => /\s/)
      assert_equal :key, tsv.identify_field("ID")
    end
  end

  def test_sort
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a B c
row2 A b C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :double, :sep => /\s/)
      assert_equal %w(row2 row1), tsv.tsv_sort{|a,b|
        a[1]["ValueA"] <=> b[1]["ValueA"]
      }.collect{|k,v| k}
      assert_equal %w(row1 row2), tsv.tsv_sort{|a,b|
        a[1]["ValueB"] <=> b[1]["ValueB"]
      }.collect{|k,v| k}
    end
  end

  def test_sort_by
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a B c
row2 A b C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :list, :sep => /\s/)
      assert_equal %w(row2 row1), tsv.sort_by("ValueA").collect{|k,v| k}
      assert_equal %w(row1 row2), tsv.sort_by("ValueB").collect{|k,v| k}
    end
  end


  def test_page
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a B f
row2 A b e
row3 A b d
row4 A b c
row5 A b b
row6 A b a
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :list, :sep => /\s/)
      assert_equal 3, tsv.page(1,3).size
      assert_equal %w(row1 row2 row3).sort, tsv.page(1,3).keys.sort
      assert_equal %w(row6 row5 row4).sort, tsv.page(1,3, "Comment").keys.sort
      assert_equal %w(row4 row3).sort, tsv.page(2,2, "Comment").keys.sort
    end
  end


  def test_sort_by_with_proc
    content =<<-EOF
#Id    ValueA    ValueB    OtherID    Pos
row1    a|aa|aaa    b    Id1|Id2    2
row2    aA    B    Id3    1
row3    A|AA|AAA|AAA    B    Id3    3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(File.open(filename), :sep => /\s+/)
      assert_equal %w(row3 row1 row2), (tsv.sort_by("ValueA") do |key, value| value.length end).collect{|k,v| k}.reverse
    end
  end

  def test_chunked_values_at
    tsv = TSV.setup({})
    10.times do |i|
      tsv[i] = i
    end
    pos = tsv.chunked_values_at (0..10-1).to_a, 2
    assert_equal (0..10-1).to_a, pos
  end

  def test_unzip
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|A    b|B    Id1|Id2
row2    aa|aa|AA|AA    b1|b2|B1|B2    Id1|Id1|Id2|Id2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
    end
  end

  def test_to_s_unmerge
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|A    b|B    Id1|Id2
row2    aa|aa|AA|AA    b1|b2|B1|B2    Id1|Id1|Id2|Id2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal 6, CMD.cmd('grep -v "#" | cut -f 1', :in => tsv.to_s(nil, false, true)).read.split("\n").length
    end
  end

  def test_to_s_no_fields
    file1 =<<-EOF
row6 dd dd ee
row1 a b c
row2 A B C
row3 1 2 3
   EOF
    tsv1 = TSV.open StringIO.new(file1), :sep => " "
    assert tsv1.to_s(:sort, true).include?('dd')
  end

  def test_to_s_unmerge_expand
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|A    b|B    Id1
row2    aa|aa|AA|AA    b1|b2|B1|B2    Id2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      text = tsv.to_s(nil, false, :expand)
      assert text =~ /row2\taa\tb2\tId2/
    end
  end

  def test_remove_duplicates
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|A|a|a    b|B|b|    Id1|Id2|Id1|Id1
row2    aa|aa|AA|AA    b1|b2|B1|B2    Id1|Id1|Id2|Id2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal %w(a A a), tsv.remove_duplicates["row1"]["ValueA"]
      assert tsv.remove_duplicates["row1"]["ValueB"].include?("")
    end

  end

  def test_unzip_zip
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|A|a|a    b|B|b|    Id1|Id2|Id1|Id1
row2    aa|aa|AA|AA    b1|b2|B1|B2    Id1|Id1|Id2|Id2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal ["b", "b", ""], tsv.unzip("ValueA", true)["row1:a"]["ValueB"]
      assert_equal ["b", "b", "", "B"].sort, tsv.unzip("ValueA", true).zip(true)["row1"]["ValueB"].sort
    end

  end
end
