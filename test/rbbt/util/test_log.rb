require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/log'

class TestLog < Test::Unit::TestCase
  def test_get_level
    assert_equal 0, Log.get_level(:debug)
    assert_equal 1, Log.get_level(:low)
    assert_equal 1, Log.get_level("LOW")
    assert_equal 1, Log.get_level(1)
    assert_equal 0, Log.get_level(nil)
  end

  def test_color
    assert Log.color(:green, "green")
  end

  def test_return_line
    puts Log.return_line  + "ccc"*3
  end
end

