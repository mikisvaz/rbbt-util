require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/attach'
require 'rbbt'

class TestAttach < Test::Unit::TestCase
  def test_attach_same_key
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ID    ValueB    OtherID
row1    b    Id1|Id2
row3    B    Id3
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach_same_key tsv2, "OtherID"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach_same_key tsv2

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :list, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach_same_key tsv2, "OtherID"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal "Id1", tsv1["row1"]["OtherID"]
  end

  def test_attach_source_field
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ValueB    OtherID
b    Id1|Id2
B    Id3
    EOF

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach_source_key tsv2, "ValueB"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :list, :sep => /\s+/)
    end

    tsv1.attach_source_key tsv2, "ValueB"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal "Id1", tsv1["row1"]["OtherID"]
  end

  def test_attach_index
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ValueE    OtherID
e    Id1|Id2
E    Id3
    EOF

    content_index =<<-EOF
#Id    ValueE
row1    e
row2    E
    EOF

    tsv1 = tsv2 = index = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content_index) do |filename|
      index = TSV.open(File.open(filename), type: :flat, :sep => /\s+/)
    end

    tsv1.attach_index tsv2, index

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :list, :sep => /\s+/)
    end

    tsv1.attach_index tsv2, index

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal "Id1", tsv1["row1"]["OtherID"]
  end

  def test_attach
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#Id    ValueB    OtherID
row1    b    Id1|Id2
row3    B    Id3
    EOF

    content3 =<<-EOF
#ValueB    OtherID
b    Id1|Id2
B    Id3
    EOF
   tsv1 = tsv2 = tsv3 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content3) do |filename|
      tsv3 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach tsv2, :fields => "OtherID"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach tsv3

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

  end

  def test_attach_using_index
    content1 =<<-EOF
#: :sep=/\\s+/
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/
#ValueE    OtherID
e    Id1|Id2
E    Id3
    EOF

    content_index =<<-EOF
#: :sep=/\\s+/
#Id    ValueE
row1    e
row2    E
    EOF

    Rbbt.claim Rbbt.tmp.test.test1.data, :string, content1
    Rbbt.claim Rbbt.tmp.test.test2.data, :string, content2
    Rbbt.claim Rbbt.tmp.test.test2.identifiers, :string, content_index

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/
    Log.tsv tsv2

    tsv2.identifiers = Rbbt.tmp.test.test2.identifiers.produce.find #.to_s

    tsv1.attach tsv2, :fields => ["OtherID"] #, :persist_input => true
    
    assert_equal tsv1.fields, %w(ValueA ValueB OtherID)
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]
  end

  def test_find_path
     content1 =<<-EOF
#: :sep=/\\s+/#:case_insensitive=false
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/#:case_insensitive=false
#OtherID ValueE
Id1|Id2    e    
Id3        E    
    EOF

    content_identifiers =<<-EOF
#: :sep=/\\s+/#:case_insensitive=false
#Id    ValueE
row1    e
row2    E
    EOF

    tsv1 = tsv2 = identifiers = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(Path.setup(filename), :key => "Id")
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(Path.setup(filename), type: :double)
    end

    TmpFile.with_file(content_identifiers) do |filename|
      identifiers = TSV.open(Path.setup(filename), type: :flat, :sep => /\s+/)
    end

    tsv1.identifiers = identifiers

    tsv1.attach tsv2
 
    assert_equal %w(ValueA ValueB ValueE), tsv1.fields
  end

  def test_merge_different_rows
    file1 =<<-EOF
row6 dd dd ee
row1 a b c
row2 A B C
row3 1 2 3
   EOF
    file2 =<<-EOF
row20 rr rr
row1 d e
row2 D E
row4 x y z
    EOF
    result =<<-EOF
row1 a b c d e
row2 A B C D E
row20    rr rr
row3 1 2 3  
row4    x y z
row6 dd dd ee  
    EOF

    TmpFile.with_file do |f|
      TSV.merge_different_fields(StringIO.new(file1), StringIO.new(file2), f, :sep => " ")
      assert_equal result, Open.read(f)
    end
  end

  def test_merge_different_rows_tsv
    file1 =<<-EOF
row6 dd dd ee
row1 a b c
row2 A B C
row3 1 2 3
   EOF
    file2 =<<-EOF
row20 rr rr
row1 d e
row2 D E
row4 x y
    EOF
    result =<<-EOF
row1 a b c d e
row2 A B C D E
row20    rr rr
row3 1 2 3  
row4    x y
row6 dd dd ee  
    EOF

    TmpFile.with_file do |f|
      tsv1 = TSV.open StringIO.new(file1), :sep => " "
      tsv2 = TSV.open StringIO.new(file2), :sep => " "
      tsv_r = tsv1.merge_different_fields tsv2
      assert_equal result, tsv_r.to_s(tsv_r.keys.sort, true).gsub(/\t/,' ')
    end
  end

  def test_merge_different_rows_split_lines
    file1 =<<-EOF
#ID,letterA,letterB,letterC
row6,dd,dd,ee
row1,a,b,c
row1,aa,bb,cc
row2,A,B,C
row3,1,2,3
   EOF
    file2 =<<-EOF
#ID,letterD,letterE
row20,rr,rr
row1,d,e
row2,D,E
row4,x,y
    EOF

    # Might be slightly different ...
    result1 =<<-EOF
#: :sep=,
#ID,letterA,letterB,letterC,letterD,letterE
row1,aa|a,bb|b,cc|c,d,e
row2,A,B,C,D,E
row20,,,,rr,rr
row3,1,2,3,,
row4,,,,x,y
row6,dd,dd,ee,,
    EOF
    result2 =<<-EOF
#: :sep=,
#ID,letterA,letterB,letterC,letterD,letterE
row1,a|aa,b|bb,c|cc,d,e
row2,A,B,C,D,E
row20,,,,rr,rr
row3,1,2,3,,
row4,,,,x,y
row6,dd,dd,ee,,
    EOF

    TmpFile.with_file do |f|
      TSV.merge_different_fields StringIO.new(file1), StringIO.new(file2), f, :sep => ','
      # ... so check for either
      assert(Open.read(f) == result1 || Open.read(f) == result2)
    end
  end

  def test_merge_different_rows_split_lines_tsv
    file1 =<<-EOF
row6,dd,dd,ee
row1,a,b,c
row1,aa,bb,cc
row2,A,B,C
row3,1,2,3
   EOF
    file2 =<<-EOF
row20,rr,rr
row1,d,e
row2,D,E
row4,x,y
    EOF
    result =<<-EOF
row1,aa|a,bb|b,cc|c,d,e
row2,A,B,C,D,E
row20,,,,rr,rr
row3,1,2,3,,
row4,,,,x,y
row6,dd,dd,ee,,
    EOF

    TmpFile.with_file do |f|
      data1 = TSV.open StringIO.new(file1), :sep => ',', :merge => true
      data2 = TSV.open StringIO.new(file2), :sep => ',', :merge => true
      data3 = data1.merge_different_fields(data2)
      data3.each do |key, list|
        list.each do |l| l.replace l.sort_by{|v| v.length}.reverse end
      end

      assert_equal result, data3.to_s(:sort, true).gsub(/\t/,',')
    end
  end

  def test_merge_rows
    file1 =<<-EOF
#ID,letterA,letterB,letterC
row1,a,b,c
row1,aa,bb,cc
row2,A,B,C
row3,1,2,3
    EOF
    TmpFile.with_file(file1) do |input|
      TmpFile.with_file() do |output|
        TSV.merge_row_fields Open.open(input), output, :sep => ','
        assert Open.read(output) =~ /^#ID,letterA,letterB,letterC$/
        assert Open.read(output).index "a|aa"
      end
    end


  end

  def test_one2one
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
row3    A    b|bb
    EOF

    content2 =<<-EOF
#ValueB    OtherID
b    Id1|Id2
B    Id3
bb   Id4
    EOF
   tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    tsv1.attach tsv2, :one2one => true

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id4), tsv1["row3"]["OtherID"]
  end

  def test_attach_flat
    content1 =<<-EOF
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#ValueA    OtherID
a    Id1|Id2
A    Id3
    EOF

    tsv1 = tsv2 = index = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :flat, :fields => ["ValueA"], :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => /\s+/)
    end

    res = tsv1.attach tsv2, :fields => ["OtherID"]
    assert res["row2"].include? "Id3"
    assert ! res["row2"].include?("b")
  end

  def test_attached_parenthesis

    content1 =<<-EOF
#Id,ValueA,bar (ValueB)
row1,a|aa|aaa,b
row2,A,B
    EOF

    content2 =<<-EOF
#foo (ValueA),OtherID
a,Id1|Id2
A,Id3
    EOF

    content3 =<<-EOF
#ValueB,ValueC
b,c
B,C
    EOF

    content4 =<<-EOF
#foobar (Id),ValueD
row1,d
row5,D
    EOF

    tsv1 = tsv2 = tsv3 = tsv4 = index = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => ',')
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => ',')
    end

    TmpFile.with_file(content3) do |filename|
      tsv3 = TSV.open(File.open(filename), type: :double, :sep => ',')
    end

    TmpFile.with_file(content4) do |filename|
      tsv4 = TSV.open(File.open(filename), type: :double, :sep => ',')
    end


    assert_equal tsv1.attach(tsv2)["row1"]["OtherID"], %w(Id1 Id2)
    assert_equal tsv1.attach(tsv2)["row2"]["OtherID"], %w(Id3)

    assert_equal tsv1.attach(tsv4)["row1"]["ValueD"], %w(d)
    assert_equal tsv4.attach(tsv1)["row1"]["bar (ValueB)"], %w(b)
    assert_equal tsv3.attach(tsv1)["b"]["ValueD"], %w(d)
  end

  def test_attach_single_nils
    content1 =<<-EOF
#Id,ValueA
row1,
row2,AA
    EOF
    content2 =<<-EOF
#Id,ValueB
row1,B
row2,BB
    EOF
    content3 =<<-EOF
#Id,ValueC
row1,
row2,CC
    EOF

    tsv1 = tsv2 = tsv3 = tsv4 = index = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.open(File.open(filename), type: :double, :sep => ',', :type => :single)
      tsv1.keys.each{|k| tsv1[k] = nil if tsv1[k] == ""}
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.open(File.open(filename), type: :double, :sep => ',', :type => :single)
      tsv2.keys.each{|k| tsv2[k] = nil if tsv2[k] == ""}
    end

    TmpFile.with_file(content3) do |filename|
      tsv3 = TSV.open(File.open(filename), type: :double, :sep => ',', :type => :single)
      tsv3.keys.each{|k| tsv3[k] = nil if tsv3[k] == ""}
    end

    assert_equal [nil, "B", nil], tsv1.attach(tsv2, :complete => true).attach(tsv3, :complete => true)["row1"]
  end

  def test_attach_index_both_non_key
    content1 =<<-EOF
#: :sep=/\\s+/
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/
#ValueE    OtherID
e    Id1|Id2
E    Id3
    EOF

    content_index =<<-EOF
#: :sep=/\\s+/
#ValueA    OtherID
a    Id1
A    Id3
    EOF

    Rbbt.claim Rbbt.tmp.test.test1.data, :string, content1
    Rbbt.claim Rbbt.tmp.test.test2.data, :string, content2
    Rbbt.claim Rbbt.tmp.test.test2.identifiers, :string, content_index

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/

    tsv2.identifiers = Rbbt.tmp.test.test2.identifiers.produce(true).produce.find #.to_s

    tsv1.attach tsv2, :fields => ["ValueE"] #, :persist_input => true
    Log.tsv tsv1
    ppp tsv1
    
  end

  def test_attach_both_non_key
    content1 =<<-EOF
#: :sep=/\\s+/
#Id    ValueA    ValueB
row1    a|aa|aaa    b
row2    A    B
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/
#ValueE    ValueB
e    b
E    B
    EOF
    Rbbt.claim Rbbt.tmp.test.test1.data, :string, content1
    Rbbt.claim Rbbt.tmp.test.test2.data, :string, content2

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/

    tsv1.attach tsv2, :fields => ["ValueE"] #, :persist_input => true
    Log.tsv tsv1
    
  end

  def test_attach_complete
    content1 =<<-EOF
#: :sep=/\\s+/
#Id    ValueA
row1    a|aa|aaa
row2    A
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/
#Id    ValueB
row1    b
row3    C
    EOF
    Rbbt.claim Rbbt.tmp.test.test1.data, :string, content1
    Rbbt.claim Rbbt.tmp.test.test2.data, :string, content2

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/

    tsv1.attach tsv2, :complete => true
    assert_equal [[], ["C"]], tsv1["row3"]

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/

    ppp tsv1.attach tsv2, :complete => ["AA"]
    tsv1.attach tsv2, :complete => ["AA"]
    assert_equal [["AA"], ["C"]], tsv1["row3"]
  end

  def test_attach_complete_identifiers
    content1 =<<-EOF
#: :sep=/\\s+/
#Id    ValueA
row1    a|aa|aaa
row2    A
    EOF

    content2 =<<-EOF
#: :sep=/\\s+/
#Id2    ValueB
ROW_1    b
ROW_2    C
    EOF

    identifiers =<<-EOF
#: :sep=/\\s+/
#Id    Id2
row1    ROW_1
row2    ROW_2
row3    ROW_3
    EOF
    Rbbt.claim Rbbt.tmp.test.test1.data, :string, content1
    Rbbt.claim Rbbt.tmp.test.test2.data, :string, content2
    Rbbt.claim Rbbt.tmp.test.identifiers.data, :string, identifiers

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/
    ids = Rbbt.tmp.test.identifiers.data.produce(true).tsv type: :double,  :sep => /\s+/

    tsv1.identifiers = ids

    tsv1.attach tsv2
    assert_equal [["A"], ["C"]], tsv1["row2"]

    tsv1 = Rbbt.tmp.test.test1.data.produce(true).tsv type: :double,  :sep => /\s+/
    tsv2 = Rbbt.tmp.test.test2.data.produce(true).tsv type: :double,  :sep => /\s+/
    ids = Rbbt.tmp.test.identifiers.data.produce(true).tsv type: :double,  :sep => /\s+/

    tsv1.identifiers = ids

    tsv1.attach tsv2, :complete => true
    assert_equal [["A"], ["C"]], tsv1["row2"]
  end
end

