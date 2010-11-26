require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'rbbt'
require 'rbbt/util/misc'

class TestBase < Test::Unit::TestCase
  def test_url
    Rbbt.add_datafiles :test => ['test', 'http://google.com']
    assert(Misc.fixutf8(File.open(Rbbt.find_datafile('test')).read) =~ /html/)
    FileUtils.rm Rbbt.find_datafile('test')
  end
end
