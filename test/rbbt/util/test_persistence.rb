require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/persistence'

class TestPersistence < Test::Unit::TestCase

  def test_string
    string = "test string"
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :string, :persistence_file => f) do string end
      assert File.exists? f
      assert_equal string, Open.read(f)

      FileUtils.rm f
    end
  end

  def test_yaml
    object = [1,2,2]
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :yaml, :persistence_file => f) do object end
      assert File.exists? f
      assert_equal object, YAML.load(File.open(f))
      assert_equal YAML.dump(object), Open.read(f)
      
      FileUtils.rm f
    end
  end

  def test_marshal
    object = [1,2,2]
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :marshal, :persistence_file => f) do object end
      assert File.exists? f
      assert_equal object, Marshal.load(File.open(f))
      assert_equal Marshal.dump(object), Open.read(f)
      
      FileUtils.rm f
    end
  end

  def test_tsv
    object = {:a => 1, :b => 2}
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :tsv_extra, :persistence_file => f) do 
        [object, {:fields => ["Number"], :key_field => "Letter", :type => :single, :cast => :to_i, :filename => "foo"}]
      end

      assert File.exists? f
      new, extra = Persistence.persist("token_file", :Test, :tsv_extra, :persistence_file => f)

      assert_equal 1, new["a"]
      assert_equal "Letter", new.key_field
      
      FileUtils.rm f
    end
  end
  
  def test_tsv2
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :sep => /\s+/, :key => "OtherID")
      tsv2 = Persistence.persist_tsv_string(tsv, 'Test', {}) do tsv end
      tsv2 = Persistence.persist_tsv_string(tsv, 'Test', {}) do tsv end

      (Object::TSV::EXTRA_ACCESSORS + [:fields, :key_field]).each do |key|
        assert_equal tsv.send(key), tsv2.send(key)
      end
      tsv.each do |key,values|
        assert_equal values, tsv2[key]
      end
    end
  end
  
  def test_tsv3
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :sep => /\s+/, :key => "OtherID")
      tsv2 = Persistence.persist_tsv(tsv, 'Test', {}) do tsv end
      tsv2 = Persistence.persist_tsv(tsv, 'Test', {}) do tsv end

      (Object::TSV::EXTRA_ACCESSORS + [:fields, :key_field]).each do |key|
        assert_equal tsv.send(key), tsv2.send(key)
      end
      tsv.each do |key,values|
        assert_equal values, tsv2[key]
      end
    end
  end

  def test_tsv4
    content =<<-EOF
#Id    ValueA    ValueB    OtherID
row1    a|aa|aaa    b    Id1|Id2
row2    A    B    Id3
    EOF

    TmpFile.with_file(content) do |filename|
      tsv  = Persistence.persist_tsv(filename, 'Test', {}) do TSV.new(filename, :sep => /\s+/, :key => "OtherID") end
      tsv2 = Persistence.persist_tsv(filename, 'Test', {}) do tsv end

      (Object::TSV::EXTRA_ACCESSORS + [:fields, :key_field]).each do |key|
        assert_equal tsv.send(key), tsv2.send(key)
      end
      tsv.each do |key,values|
        assert_equal values, tsv2[key]
      end
    end
  end

  def test_integer
    content =<<-EOF
#Id    ValueA
row1   1
row2   2 
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :single, :sep => /\s+/, :cast => :to_i, :persistence => true)
      assert_equal 1, tsv["row1"]
    end
  end

  def test_integer
    content =<<-EOF
row1   1 2 3 4 5
row2   2 4 6 8
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.new(filename, :flat, :sep => /\s+/, :cast => :to_i, :persistence => true)
      assert_equal [1,2,3,4,5], tsv["row1"]
    end
  end

  def test_persist_dir
    string = Persistence.persist("Test", :Test, :string, :persistence_dir => Rbbt.tmp.test.persistence) do
      "Test"
    end

    assert Dir.glob(Rbbt.tmp.test.persistence.find + '*').length == 1

    assert_equal "Test", string
  end

end

