require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/log'

class TestLog < Test::Unit::TestCase
  def test_color
    assert Log.color(:green, "green")
  end
end

