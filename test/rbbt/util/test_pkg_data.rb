require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt'
require 'rbbt/util/pkg_data'

class TestPKGData < Test::Unit::TestCase
  def test_claims
    begin
      assert Rbbt.claims.empty?
      Rbbt.claim :foo, "bar"
      assert_equal 1, Rbbt.claims.length
    rescue
      Rbbt.declaim Rbbt.files.foo
    end
  end

  def test_path
    assert_equal File.join(Rbbt.datadir, 'Organism/Hsa'), Rbbt.files.Organism.Hsa
    Rbbt.files.Organism.Hsa.identifiers.produce
  end

  def test_claim_proc
    begin
      assert_nil Rbbt.reclaim(Rbbt.files.foo)

      Rbbt.claim :foo, proc{"bar"}
      assert_not_nil Rbbt.reclaim Rbbt.files.foo

      assert Hash === Rbbt.reclaim(Rbbt.files.foo).last
      assert_equal "bar", Rbbt.files.foo.read
    ensure
      Rbbt.declaim Rbbt.files.foo
      FileUtils.rm Rbbt.files.foo
    end
  end

  def test_claim_cp
    begin
      Open.write File.join(Rbbt.rootdir, 'share', 'foo'), "bar"
      Rbbt.claim :foo
      assert_equal "bar", Rbbt.files.foo.read
    ensure
      Rbbt.declaim Rbbt.files.foo
      FileUtils.rm Rbbt.files.foo if File.exists? Rbbt.files.foo
      FileUtils.rm File.join(Rbbt.rootdir, 'share', 'foo') if File.exists? File.join(Rbbt.rootdir, 'share', 'foo')
    end
  end

  def test_claim_tsv
    begin
      Rbbt.claim :foo, TSV.new({:a => 1, :b => 2})
      assert File.exists? Rbbt.files.foo
      assert_equal "1", Rbbt.files.foo.tsv(:type => :single)["a"]
    ensure
      FileUtils.rm Rbbt.files.foo if File.exists? Rbbt.files.foo
    end
  end

  def test_claim_rakefile
    begin
      FileUtils.mkdir_p File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/')
      Open.write(File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile'), "file :foo do |t| Open.write(t.name, 'bar') end")
      Rbbt.claim :foo, :Rakefile, 'test/Rake' 
      assert_equal "bar", Rbbt.files.test.Rake.foo.read
    ensure
      begin
        FileUtils.rm File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test')
        FileUtils.rm Rbbt.files.test.Rake.foo 
        FileUtils.rm_r Rbbt.files.test.Rake 
        FileUtils.rm_r Rbbt.files.test
      rescue
      end
    end
  end

  def test_claim_rakefile2
    begin
      FileUtils.mkdir_p File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/')
      Open.write(File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile'), "file :foo do |t| Open.write(t.name, 'bar') end")
      Rbbt.claim :foo, "test/Rake/Rakefile", 'test' 
      assert_equal "bar", Rbbt.files.test.foo.read
    ensure
      begin
        FileUtils.rm File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test')
        FileUtils.rm Rbbt.files.test.foo 
        FileUtils.rm_r Rbbt.files.test
      rescue
      end
    end
  end

  def test_claim_rakefile3
    begin
      FileUtils.mkdir_p File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/')
      Open.write(File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile'), "file :foo do |t| Open.write(t.name, 'bar') end")
      Rbbt.claim :all, "test/Rake/Rakefile", 'test' 
      assert_equal "bar", Rbbt.files.test.foo.read
    ensure
      begin
        FileUtils.rm File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test')
        FileUtils.rm Rbbt.files.test.foo 
        FileUtils.rm_r Rbbt.files.test
      rescue
      end
    end
  end

  def test_claim_namespace_identifiers
    begin
      FileUtils.mkdir_p File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/')
      Open.write(File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile'), "
                 file :foo do |t| Open.write(t.name, 'bar') end
                 file :identifiers do |t| Open.write(t.name, 'bar') end
                 ")
      Rbbt.claim :all, "test/Rake/Rakefile", 'test' 
      assert_equal 1, Rbbt.files.test.foo.identifier_files.length
    ensure
      begin
        FileUtils.rm File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake/Rakefile')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test/Rake')
        FileUtils.rmdir File.join(PKGData.sharedir_for_file(__FILE__), 'test')
        FileUtils.rm Rbbt.files.test.foo 
        FileUtils.rm_r Rbbt.files.test
      rescue
      end
    end
  end
end

