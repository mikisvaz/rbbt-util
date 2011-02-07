require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/tsv'
require 'rbbt/util/tsv/attach'
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
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1.attach_same_key tsv2, "OtherID"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1.attach_same_key tsv2

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields

    tsv1 = tsv2 = nil
    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :list, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
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
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1.attach_source_key tsv2, "ValueB"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :list, :sep => /\s+/)
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
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content_index) do |filename|
      index = TSV.new(File.open(filename), :flat, :sep => /\s+/)
    end

    tsv1.attach_index tsv2, index

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :list, :sep => /\s+/)
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
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    TmpFile.with_file(content3) do |filename|
      tsv3 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1.attach tsv2, "OtherID"

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

    TmpFile.with_file(content1) do |filename|
      tsv1 = TSV.new(File.open(filename), :double, :sep => /\s+/)
    end

    tsv1.attach tsv3

    assert_equal %w(ValueA ValueB OtherID), tsv1.fields
    assert_equal %w(Id1 Id2), tsv1["row1"]["OtherID"]

  end

  def test_attach_using_index
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
#: :sep=/\\s+/
#Id    ValueE
row1    e
row2    E
    EOF


    require 'rbbt/sources/organism'

    Rbbt.claim "data", StringIO.new(content1), "Test1"
    Rbbt.claim "data", StringIO.new(content2), "Test2"
    Rbbt.claim "identifiers", StringIO.new(content_index), "Test2"

    tsv1 = tsv2 = nil

    tsv1 = Rbbt.files.Test1.data.tsv :double,  :sep => /\s+/
    tsv2 = Rbbt.files.Test2.data.tsv :double,  :sep => /\s+/

    tsv2.identifiers = Rbbt.files.Test2.identifiers

    tsv1.attach tsv2, "OtherID", :in_namespace => false

    assert_equal tsv1.fields,%w(ValueA ValueB OtherID)
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
      tsv1 = TSV.new(File.open(filename), :key => "Id")
    end

    TmpFile.with_file(content2) do |filename|
      tsv2 = TSV.new(File.open(filename), :double)
    end

    TmpFile.with_file(content_identifiers) do |filename|
      identifiers = TSV.new(File.open(filename), :flat, :sep => /\s+/)
    end

    tsv1.identifiers = identifiers
    tsv1.attach tsv2
 
    assert_equal %w(ValueA ValueB ValueE), tsv1.fields
  end
end

