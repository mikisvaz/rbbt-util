require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/config'

class TestConfig < Test::Unit::TestCase
  def setup
    Rbbt::Config.set({:cpus => 30}, :test_config, :test)
    Rbbt::Config.set(:cpus, 5, "slow::2", :test)
    Rbbt::Config.set({:token => "token"}, "token", "key:token")
    Rbbt::Config.set(:notoken, "no_token")
  end

  def test_simple
    assert_equal 30, Rbbt::Config.get(:cpus, :test_config)
  end

  def test_match
    assert_equal({20 => ["token"]}, Rbbt::Config.match({["key:token"] => "token"}, "key:token"))
  end

  def test_simple_no_token
    assert_equal "token", Rbbt::Config.get("token", "token")
    assert_equal "token", Rbbt::Config.get("token")
    assert_equal "no_token", Rbbt::Config.get("notoken")
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

  def test_order
    Rbbt::Config.add_entry 'key', 'V1', 'token1'
    Rbbt::Config.add_entry 'key', 'V2', 'token2'
    Rbbt::Config.add_entry 'key', 'V3', 'token2'

    assert_equal "V3", Rbbt::Config.get('key', 'token2')
    assert_equal "V1", Rbbt::Config.get('key', 'token1')
    assert_equal "V3", Rbbt::Config.get('key', 'token2', 'token1')
    assert_equal "V1", Rbbt::Config.get('key', 'token1', 'token2')
  end


end

