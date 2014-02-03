$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))

require 'test/unit'
require 'fileutils'
require 'rubygems'

require 'rbbt'
require 'rbbt/resource/path'


class Test::Unit::TestCase
  include FileUtils

  def setup
    Random.new
    if defined? Persist
      Persist.cachedir = Rbbt.tmp.test.persistence.find :user
    end
  end

  def teardown
    if defined? Persist
      FileUtils.rm_rf Path.setup("", 'rbbt').tmp.test.find :user
      Persist::CONNECTIONS.values.each do |c| c.close end
      Persist::CONNECTIONS.clear
    end
  end

  def datafile_test(file)
    File.join(File.dirname(__FILE__), 'data', file)
  end
end
