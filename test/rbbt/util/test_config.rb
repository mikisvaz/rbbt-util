require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/config'

class TestConfig < Test::Unit::TestCase
  def setup
    Rbbt::Config.set({:cpus => 30}, :test_config, :test)
    Rbbt::Config.set(:cpus , 5, "slow::2", :test)
  end

  def test_simple
    assert_equal 30, Rbbt::Config.get(:cpus, :test_config)
  end

  def test_prio
    assert_equal 5, Rbbt::Config.get(:cpus, :slow, :test)
  end
end

