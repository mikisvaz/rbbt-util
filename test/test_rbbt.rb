require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'rbbt'
require 'rbbt/util/misc'

class TestRbbt < Test::Unit::TestCase
  def test_url
    Rbbt.add_datafiles :test => ['test', 'http://google.com']
    assert(Misc.fixutf8(File.open(Rbbt.find_datafile('test')).read) =~ /html/)
    FileUtils.rm_rf File.dirname(Rbbt.find_datafile('test'))
  end

  def test_proc
    Rbbt.add_datafiles :test => ['test', proc{'test'}]
    assert(File.open(Rbbt.find_datafile('test')).read == 'test')
    FileUtils.rm_rf File.dirname(Rbbt.find_datafile('test'))
  end

  def test_Rakefile
    sharedir = PKGData.sharedir_for_file(__FILE__)
    
    FileUtils.mkdir_p File.join(sharedir, 'install', 'rake')
    File.open(File.join(sharedir, 'install', 'rake', 'Rakefile'), 'w') do |f|
      f.puts "task :default do File.open('foo', 'w') do |f| f.puts 'bar' end end"
    end

    Rbbt.add_datafiles :rake => ['', 'rake']

    assert_equal 'bar', File.open(File.join(Rbbt.datadir, 'rake', 'foo')).read.chomp

    FileUtils.rm_rf  File.join(sharedir, 'install', 'rake')
    FileUtils.rm_rf  File.join(Rbbt.datadir, 'rake')
  end

  def test_Rakefile_with_file
    sharedir = PKGData.sharedir_for_file(__FILE__)
    
    FileUtils.mkdir_p File.join(sharedir, 'install', 'rake')
    File.open(File.join(sharedir, 'install', 'rake', 'Rakefile'), 'w') do |f|
      f.puts "\
task :default do File.open('foo', 'w') do |f| f.puts 'bar' end end
task 'file1' do |t| File.open(t.name, 'w') do |f| f.puts 'file 1' end end
      "
    end

    Rbbt.add_datafiles 'rake/file1' => ['', 'rake']

    assert(! File.exists?(File.join(Rbbt.datadir, 'rake', 'foo')))
    assert(File.exists?(File.open(File.join(Rbbt.datadir, 'rake', 'file1'))))
    assert_equal('file 1', File.open(File.join(Rbbt.datadir, 'rake', 'file1')).read.chomp)
    assert_equal('file 1', File.open(Rbbt.find_datafile('rake/file1')).read.chomp)

    FileUtils.rm_rf  File.join(sharedir, 'install', 'rake')
    FileUtils.rm_rf  File.join(Rbbt.datadir, 'rake')
  end

end
