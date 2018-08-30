gem "test-unit", "~> 3.0"
gem "minitest", "~> 5.5"

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

    Persist.cachedir = Rbbt.tmp.test.persistence.find :user if defined? Persist

    Entity.entity_property_cache = Rbbt.tmp.test.entity_property.find(:user) if defined? Entity
  end

  def teardown
    FileUtils.rm_rf Rbbt.tmp.test.workflow.find
    #Open.clear_dir_repos
    #if defined? Persist
    #  FileUtils.rm_rf Path.setup("", 'rbbt').tmp.test.find :user
    #  Persist::CONNECTIONS.values.each do |c| c.close end
    #  Persist::CONNECTIONS.clear
    #end

    #if defined? Entity
    #  FileUtils.rm_rf Entity.entity_property_cache.find(:user) if Entity.entity_property_cache =~ /tmp\/test/
    #end
  end

  def self.datafile_test(file)
    Path.setup(File.join(File.dirname(__FILE__), 'data', file.to_s))
  end

  def datafile_test(file)
    Test::Unit::TestCase.datafile_test(file)
  end
end
