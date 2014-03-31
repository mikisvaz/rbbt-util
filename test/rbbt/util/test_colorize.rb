require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/colorize'
require 'test/unit'

class TestColorize < Test::Unit::TestCase
  def test_color_array
    a = (1..16).to_a

    assert_equal 16, Colorize.distinct(a).length
    assert_equal 16, Colorize.distinct(a).compact.uniq.length
  end
end
