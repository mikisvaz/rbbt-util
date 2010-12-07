require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt'
require 'rbbt/util/data_module'
require 'test/unit'
require 'fileutils'

module DataTest  
  extend DataModule

  def self.salute(name)
    "Hello #{name}"
  end

  World = with_key("world")
end

class TestDataModule < Test::Unit::TestCase

  SHAREDIR = File.join(PKGData.sharedir_for_file(__FILE__), 'install', 'DataTest')

  def setup
    FileUtils.mkdir_p SHAREDIR
    File.open(File.join(SHAREDIR, 'Rakefile'), 'w') do |f|
      f.puts "task :file1 do |t| File.open(t.name, 'w') do |f| f.write 'File 1' end end"
    end
  end

  def test_module
    assert_equal "File 1", DataTest.file1.chomp
    assert_equal "Hello world", DataTest.salute("world")
    assert_equal "Hello world", DataTest.salute("world")
    assert_equal "Hello world", DataTest::with_key("world").salute
    assert_equal "Hello world", DataTest::World.salute
    FileUtils.rm_rf File.join(Rbbt.datadir, 'DataTest')
  end

  def test_method_missing
    assert_raise NoMethodError do DataTest.missing end
  end

  def teardown
    FileUtils.rm_rf SHAREDIR
  end
end

