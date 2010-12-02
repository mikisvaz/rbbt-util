require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'rbbt'
require 'rbbt/util/misc'

class TestBase < Test::Unit::TestCase
  def test_url
    Rbbt.add_datafiles :test => ['test', 'http://google.com']
    assert(Misc.fixutf8(File.open(Rbbt.find_datafile('test')).read) =~ /html/)
    FileUtils.rm Rbbt.find_datafile('test')
  end

  def test_Rakefile
    sharedir = Rbbt.sharedir(__FILE__)
    
    FileUtils.mkdir_p File.join(sharedir, 'install', 'rake')
    File.open(File.join(sharedir, 'install', 'rake', 'Rakefile'), 'w') do |f|
      f.puts "task :default do File.open('foo', 'w') do |f| f.puts 'bar' end end"
    end

    Rbbt.add_datafiles :rake => ['', 'rake']

    assert_equal 'bar', File.open(File.join(Rbbt.datadir, 'rake', 'foo')).read.chomp

    FileUtils.rm_rf  File.join(sharedir, 'install', 'rake')
    FileUtils.rm_rf  File.join(Rbbt.datadir, 'rake')
  end
end
