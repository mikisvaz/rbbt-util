require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt'
require 'rbbt/util/data_module'
require 'test/unit'
require 'fileutils'

SHAREDIR = File.join(PKGData.sharedir_for_file(__FILE__), 'install/DataTest')
FileUtils.mkdir_p SHAREDIR
File.open(File.join(SHAREDIR, 'Rakefile'), 'w') do |f|
  f.puts "file :file1 do |t| File.open(t.name, 'w') do |f| f.write 'File 1' end end"
  f.puts "file :tsv_file do |t| File.open(t.name, 'w') do |f| f.write 'a\t1\nb\t2\n' end end"
end

module DataTest  
  extend DataModule

  def self.salute(name)
    "Hello #{name}"
  end

  World = with_key("world")
end

class TestDataModule < Test::Unit::TestCase

  def setup

    FileUtils.mkdir_p SHAREDIR
    File.open(File.join(SHAREDIR, 'Rakefile'), 'w') do |f|
      f.puts "file :file1 do |t| File.open(t.name, 'w') do |f| f.write 'File 1' end end"
      f.puts "file :tsv_file do |t| File.open(t.name, 'w') do |f| f.write 'a\t1\nb\t2\n' end end"
    end
  end

  def test_rakefile
    assert_equal Rbbt.files.DataTest, DataTest.datadir
    assert_equal "File 1", Rbbt.files.DataTest.file1.read
    assert_equal "Hello world", DataTest.salute("world")
    assert_equal "Hello world", DataTest::with_key("world").salute
    assert_equal "Hello world", DataTest::World.salute
    assert_equal "DataTest", Rbbt.files.DataTest.tsv_file.namespace
    assert_equal "DataTest", Rbbt.files.DataTest.tsv_file.tsv.namespace
    FileUtils.rm_rf File.join(Rbbt.datadir, 'DataTest')
  end

  def teardown
    FileUtils.rm_rf SHAREDIR
  end
end

