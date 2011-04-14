require 'test/unit'
$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'rbbt'
require 'rbbt/util/persistence'
require 'rbbt/util/tmpfile'
require 'rbbt/util/log'
require 'fileutils'

class Test::Unit::TestCase
  include FileUtils

  def setup
    Persistence.cachedir = Rbbt.tmp.test.persistence.find :user
  end

  def teardown
    FileUtils.rm_rf Rbbt.tmp.test.find :user
  end

  def datafile_test(file)
    File.join(File.dirname(__FILE__), 'data', file)
  end
end
