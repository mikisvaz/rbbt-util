require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/persistence'

class TestPersistence < Test::Unit::TestCase

  def test_string
    string = "test string"
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :string, :persistence_file => f) do string end
      assert File.exists? f
      assert_equal string, Open.read(f)

      rm f
    end
  end

  def test_yaml
    object = [1,2,2]
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :yaml, :persistence_file => f) do object end
      assert File.exists? f
      assert_equal object, YAML.load(File.open(f))
      assert_equal YAML.dump(object), Open.read(f)
      
      rm f
    end
  end

  def test_marshal
    object = [1,2,2]
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :marshal, :persistence_file => f) do object end
      assert File.exists? f
      assert_equal object, Marshal.load(File.open(f))
      assert_equal Marshal.dump(object), Open.read(f)
      
      rm f
    end
  end

  def test_tsv
    object = {:a => 1, :b => 2}
    TmpFile.with_file do |f|
      Persistence.persist("token_file", :Test, :tsv, :persistence_file => f) do 
        [object, {:fields => ["Number"], :key_field => "Letter", :type => :list, :filename => "foo"}]
      end

      assert File.exists? f
      new, extra = Persistence.persist("token_file", :Test, :tsv, :persistence_file => f)

      assert_equal 1, new["a"]
      assert_equal "Letter", new.key_field
      
      rm f
    end
  end


end

