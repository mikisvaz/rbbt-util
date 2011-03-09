require 'test/unit'
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'rbbt/util/tmpfile'
require 'rbbt/util/log'
require 'fileutils'

class Test::Unit::TestCase
  include FileUtils

  def teardown
    FileUtils.rm_rf Rbbt.tmp.test.find :user
  end

  def test_datafile(file)
    File.join(File.dirname(__FILE__), 'data', file)
  end
end
