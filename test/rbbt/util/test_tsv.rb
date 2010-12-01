require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'

class TestTSV < Test::Unit::TestCase
  def test_keep_empty
    content =<<-EOF
#Id ValueA ValueB Comment
row1 a|aa|aaa b c
row2 A B 
    EOF

    TmpFile.with_file(content) do |filename|
      data, key_field, fields = TSV.parse(File.open(filename), :sep => /\s+/, :keep_empty => true)
      assert_equal ["ValueA", "ValueB", "Comment"], fields
      assert_equal ["c"], data["row1"][2]
      assert_equal [""], data["row2"][2]
    end
  end

  def test_slice
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s/)
      assert_equal [["a"],["c"]], tsv.slice("ValueA", "Comment")["row1"]
    end
  end

  def test_headers
    content =<<-EOF
#ID ValueA ValueB Comment
row1 a b c
row2 A B C
    EOF

    TmpFile.with_file(content) do |filename|
      assert_equal ['ID', 'ValueA', 'ValueB', 'Comment'], TSV.headers(filename, :sep => ' ')
    end
  end


  def test_hash
    content =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    TmpFile.with_file(content) do |filename|
      data, key_field, fields = TSV.parse(File.open(filename), :sep => /\s+/)
      assert_equal "Id", key_field
      assert_equal ["ValueA", "ValueB"], fields
      assert_equal ["a", "aa", "aaa"], data["row1"][0]
    end
  end

  def test_large
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :large => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_tsv
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID")
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["Id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_extra
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :extra => 2)
      assert_equal ["b"], tsv["Id2"][0]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :extra => 'ValueB')
      assert_equal ["b"], tsv["Id2"][0]
    end
  end

  def test_case
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :case_insensitive => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"][1]
    end
  end

  def test_persistence
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      tsv['Id4'] = [["row3"],["aA"],["bB","bbBB"]]
      assert_equal ["aA"], tsv["Id4"][1]
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["aA"], tsv["Id4"][1]
      assert_equal [["b"],["B"]], tsv.values_at("Id1", "Id3").collect{|values| values[2]}
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true, :flatten => true)
      assert(tsv["Id3"].include? "A")
    end
  end

  def test_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end
  end

  def test_best_index
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b|A    Id1
row2    A    a|B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => false, :order => true)
      assert_equal "Id1", index['a'].first
      assert_equal "Id3", index['A'].first
      assert_equal "OtherID", index.key_field
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID")
      index = tsv.index(:case_insensitive => true)
      assert index["row1"].include? "Id1"
      assert_equal "OtherID", index.key_field
    end
  end

  def test_values_at
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :persistence => true)
      index = tsv.index(:case_insensitive => true)
      assert index.values_at(*["row1"]).first.include? "Id1"
    end
  end

  def test_named_array
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/, :native => "OtherID", :case_insensitive => true)
      assert_equal "OtherID", tsv.key_field
      assert_equal ["Id", "ValueA", "ValueB"], tsv.fields
      assert_equal ["a", "aa", "aaa"], tsv["id1"][1]
      assert_equal ["a", "aa", "aaa"], tsv["Id2"]["ValueA"]

      tsv_sliced = tsv.slice("ValueA", "ValueB")
      assert_equal ["ValueA", "ValueB"], tsv_sliced.fields
      assert_equal ["a", "aa", "aaa"], tsv_sliced["id1"][0]
      assert_equal ["a", "aa", "aaa"], tsv_sliced["Id2"]["ValueA"]
    end
  end

  begin
    def _test_helpers
      require 'rbbt/sources/organism'
      filename = File.join(Organism.datadir('Sce'), 'identifiers')

      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index['1020'].include? 'CDK5'
      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index[[nil,'1020']].include? 'CDK5'
      index = TSV.index(filename, :persistence => true, :native => "Associated Gene Name")
      assert index[['MISSING','1020']].include? 'CDK5'
    end
  rescue
  end


  def test_sort
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal "row2", tsv.sort{|a,b| a[1]["ValueB"] <=> b[1]["ValueA"] }.first[0]
      assert_equal "B", tsv.sort{|a,b| a[1]["ValueB"] <=> b[1]["ValueA"] }.first[1]["ValueB"].first
    end

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(File.open(filename), :sep => /\s+/)
      assert_equal "row2", tsv.sort_by{|k,v| v["ValueB"]}.first[0]
      assert_equal "B", tsv.sort_by{|k,v| v["ValueB"]}.first[1]["ValueB"].first
    end
  end
end

