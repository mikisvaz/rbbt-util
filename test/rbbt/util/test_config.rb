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

  def test_with_config
    Rbbt::Config.add_entry 'key', 'valueA', 'token'
    assert_equal "valueA", Rbbt::Config.get('key', 'token')
    assert_equal "default", Rbbt::Config.get('key2', 'token', :default => 'default')

    Rbbt::Config.with_config do 
      Rbbt::Config.add_entry 'key', 'valueB', 'token'
      Rbbt::Config.add_entry 'key2', 'valueB2', 'token'
      assert_equal "valueB", Rbbt::Config.get('key', 'token')
      assert_equal "valueB2", Rbbt::Config.get('key2', 'token', :default => 'default')
    end

    assert_equal "valueA", Rbbt::Config.get('key', 'token')
    assert_equal "default", Rbbt::Config.get('key2', 'token', :default => 'default')
  end
end

