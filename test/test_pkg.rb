require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'rbbt'
require 'rbbt/util/pkg_data'
require 'rbbt/util/pkg_config'
require 'rbbt/util/data_module'
require 'yaml'

module A
  extend PKGConfig
  extend PKGData

  self.load_cfg(%w(datadir), {"datadir" => "/tmp/A"}.to_yaml)
end

module B
  extend PKGConfig
  extend PKGData

  self.load_cfg(%w(datadir), {"datadir" => "/tmp/B"}.to_yaml)
end

module DA
  PKG=A
  extend DataModule
end

module DB
  PKG=B
  extend DataModule
end

class TestPKG < Test::Unit::TestCase
  def test_datadir
    assert_equal "/tmp/A", A.datadir
    assert_equal "/tmp/B", B.datadir
  end

end
