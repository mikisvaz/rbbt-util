require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/tsv'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestTSV < Test::Unit::TestCase

  def test_extend
    a = {
      "one" => "1", 
      "two" => "2"
    }

    a.extend TSV
    
    a.key_field = "Number"

    assert_equal "1", a["one"]
  end
  
  def test_tsv_1
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal ["ValueA", "ValueB", "OtherID"], tsv.fields
      assert_equal :double, tsv.type
      assert_equal "Id", tsv.key_field

      tsv = TSV.open(filename, :sep => /\s+/, :key_field => "Id")
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal ["ValueA", "ValueB", "OtherID"], tsv.fields
      assert_equal :double, tsv.type
      assert_equal "Id", tsv.key_field
 
    end
  end

  def test_headerless
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal :double, tsv.type
    end
  end

  def test_headerless_fields
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :fields => [1])
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal :double, tsv.type
      assert_equal [%w(a aa aaa)], tsv["row1"]
    end
  end

  def test_tsv_persistence
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :persist =>  true)
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal ["ValueA", "ValueB", "OtherID"], tsv.fields
      assert_equal :double, tsv.type
      assert_equal "Id", tsv.key_field
      assert TokyoCabinet::HDB === tsv
    end
  end

  def test_tsv_field_selection
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :key_field => 0)
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]

      tsv = TSV.open(filename, :sep => /\s+/, :key_field => "Id")
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]

      tsv = TSV.open(filename, :sep => /\s+/, :fields => 1)
      assert_equal ["a", "aa", "aaa"], tsv["row1"].first

      tsv = TSV.open(filename, :sep => /\s+/, :fields => 2)
      assert_equal ["b"], tsv["row1"].first

      tsv = TSV.open(filename, :sep => /\s+/, :fields => [1,2])
      assert_equal ["a", "aa", "aaa"], tsv["row1"].first
      assert_equal ["b"], tsv["row1"].last

      tsv = TSV.open(filename, :sep => /\s+/, :fields => [1,3])
      assert_equal ["a", "aa", "aaa"], tsv["row1"].first
      assert_equal ["Id1", "Id2"], tsv["row1"].last

      tsv = TSV.open(filename, :sep => /\s+/, :key_field => "OtherID")
      assert_equal ["row1"], tsv["Id1"].first
      assert_equal tsv["Id2"], tsv["Id1"]

      tsv = TSV.open(filename, :sep => /\s+/, :key_field => "OtherID", :fields => "Id")
      assert_equal ["row1"], tsv["Id1"].first
      assert_equal tsv["Id2"], tsv["Id1"]

      tsv = TSV.open(filename, :sep => /\s+/, :key_field => "OtherID", :fields => ["ValueA",2])
      assert_equal ["a", "aa", "aaa"], tsv["Id1"].first
      assert_equal tsv["Id2"], tsv["Id1"]
    end
  end

  def test_tsv_cast
    content =<<-EOF
#Id    Value
row1    1|2|3
row2    4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :cast => :to_i)
      assert_equal [1,2,3], tsv["row1"][0]
      tsv = TSV.open(filename, :sep => /\s+/, :cast => :to_f)
      assert_equal [1.0,2.0,3.0], tsv["row1"][0]
    end
  end

  def test_tsv_single
    content =<<-EOF
#Id    Value
row1    1
row2    4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :cast => :to_i, :type => :single, :fields => "Value")
      assert_equal 1, tsv["row1"]
      tsv = TSV.open(filename, :sep => /\s+/, :cast => :to_i, :type => :single, :fields => ["Value"])
      assert_equal 1, tsv["row1"]
      tsv = TSV.open(filename, :sep => /\s+/, :type => :single, :key_field => "Value", :fields => ["Id"])
      assert_equal "row1", tsv["1"]
    end
  end

  def test_tsv_single_from_flat
    content =<<-EOF
#: :type=:flat
#Id    Value
row1    1 2
row2    4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :type => :single, :key_field => "Value", :fields => ["Id"])
      assert_equal "row1", tsv["1"]
    end
  end


  def test_tsv_serializer
    content =<<-EOF
#Id    Value
row1    1
row2    4
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :cast => :to_i, :type => :single, :serializer => :integer)
      assert_equal 1, tsv["row1"]
      assert String === tsv.send(:[], "row1", true)
    end
  end

  def test_tsv_header_options
    content =<<-EOF
#: :sep=/\\s+/
#Id Value
1 a
2 b
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename)
      assert_equal [["a"]], tsv["1"]
    end
  end

  def test_tsv_fastimport
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
row3    AA    B    Id3
    EOF

    TmpFile.with_file(content.gsub(/ +/,"\t")) do |filename|
      tsv = TSV.open(filename, :persist => true)
      assert_equal ["a", "aa", "aaa"], tsv["row1"][0]
      assert_equal ["ValueA", "ValueB", "OtherID"], tsv.fields
      assert_equal :double, tsv.type
      assert_equal "Id", tsv.key_field
    end
  end

  def test_header_type
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
1 a
2 b
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename)
      assert_equal :single, tsv.type
      assert_equal "a", tsv["1"]
    end
 
  end

  def test_single_cast
    content =<<-EOF
#: :sep=/\\s+/#:type=:single#:cast=:to_i
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename)
      assert_equal :single, tsv.type
      assert_equal 1, tsv["a"]
    end
  end

  def test_key_field
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value")
      assert_equal %w(Id), tsv.fields
      assert_equal "Value", tsv.key_field
      assert_equal "a", tsv["1"]
    end
  end

  def test_fix
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :fix => Proc.new{|l| if l =~ /1/;then "a 3" else l end})
      assert_equal "a", tsv["3"]
    end
  end

  def test_select
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :select => Proc.new{|l| ! l =~ /1/})
      assert(! tsv.include?("3"))
    end
  end

  def test_grep
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :grep => "#\\|2")
      assert(! tsv.include?("3"))
    end
  end

  def test_grep_invert
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :grep => "#\\|2", :invert_grep => true)
      assert(! tsv.include?("2"))
    end
  end

  def test_grep_header
    content =<<-EOF
#: :sep=/\\s+/#:type=:single#:namespace=Test
#Id Value
a 1
b 2
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :grep => "#\\|2")
      assert(! tsv.include?("1"))
      assert(tsv.include?("2"))
    end
  end

  def test_json
    content =<<-EOF
#: :sep=/\\s+/#:type=:single
#Id Value
a 1
b 2
    EOF

    require 'json'
    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Value", :grep => "#\\|2")
    end
 
  end

  def test_flat_no_merge
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :type => :flat, :fields => ["ValueA"])
      assert_equal ["a", "aa", "aaa"], tsv["row1"]
      assert_equal ["ValueA"], tsv.fields
      assert_equal :flat, tsv.type
      assert_equal "Id", tsv.key_field
    end
  end

  def test_flat_merge
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row1    aaaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :merge => true, :type => :flat, :fields => ["ValueA"])
      assert_equal ["a", "aa", "aaa", "aaaa"], tsv["row1"]
    end
  end

  def test_flat
    content =<<-EOF
#Id    ValueA 
row1   a   aa   aaa
row2   b  bbb bbbb bb
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :merge => false, :type => :flat, :fields => ["ValueA"])
      assert_equal ["a", "aa", "aaa"], tsv["row1"]
    end
  end

  def test_flat_key
    content =<<-EOF
#Id    ValueA 
row1   a   aa   aaa
row2   b  bbb bbbb bb aa
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :merge => true, :type => :flat, :key_field => "ValueA")
      assert_equal ["row1"], tsv["a"]
      assert_equal ["row1", "row2"], tsv["aa"]
    end
  end



  def test_zipped
    content =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b|bb|bbb
row2    a|aa|aaa    c|cc|ccc
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :merge => true, :type => :double, :key_field => "ValueA", :zipped => true)
      assert_equal [["row1", "row2"], ["b", "c"]], tsv["a"]
    end
  end

  def test_named_array_key
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/)
      assert_equal "row1", tsv["row1"].key

    end
 
  end

  def test_unnamed_key
    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :key_field => 1)
      assert tsv.keys.include? "a"
    end
 
  end

  def test_float_array
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1   0.2   0.3 0
row2    0.1  4.5 0
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :persist => true, :type => :list, :cast => :to_f)
      assert_equal [0.2, 0.3, 0], tsv["row1"]
      assert_equal :float_array, tsv.serializer
    end
 
  end

  def test_flat_field_select
    content =<<-EOF
#: :type=:flat
#Id    Value
row1   a  aa  aaa
row2    b  bb bbb
    EOF

    TmpFile.with_file(content) do |filename|
      assert TSV.open(filename, :sep => /\s+/, :key_field => "Value").include? "aa"
    end
  end

  def test_flat2
    content =<<-EOF
#: :type=:flat
#Id    Value
row1    a|aa|aaa
row2    A|AA|AAA
    EOF

    TmpFile.with_file(content) do |filename|
      assert TSV.open(filename, :sep => /\s+/, :type => :flat).include? "row1"
      assert TSV.open(filename, :sep => /\s+/, :type => :flat)["row1"].include? "a"
      assert TSV.open(filename, :sep => /\s+/, :type => :flat, :key_field => "Id")["row1"].include? "a"
      assert TSV.open(filename, :sep => /\s+/, :type => :flat, :key_field => "Id", :fields => ["Value"])["row1"].include? "a"
    end
  end


  def test_tsv_flat_double
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :sep => /\s+/, :type => :flat, :key_field => "ValueA", :fields => ["OtherID"], :merge => true)
      assert tsv["aaa"].include? "Id1"
      assert tsv["aaa"].include? "Id2"
 
    end
  end

  def test_flat2single
    content =<<-EOF
#: :type=:flat
#Id    Value
row1    a aa aaa
row2    A AA AAA
    EOF

    TmpFile.with_file(content) do |filename|
      assert TSV.open(filename, :sep => /\s+/, :type => :single, :key_field => "Value").include? "aaa"
    end
 
  end

  def test_shard
    shard_function = Proc.new do |key|
      key[-1]
    end
    tsv = datafile_test('identifiers').tsv :persist => true, :shard_function => shard_function
    assert_equal 10000, tsv.keys.length + 2
  end
end
