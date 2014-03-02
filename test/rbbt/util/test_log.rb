require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/log'

class TestLog < Test::Unit::TestCase
  def test_color
    assert Log.color(:green, "green")
  end

  def test_return_line
    puts Log.return_line  + "ccc"*3
  end
end

