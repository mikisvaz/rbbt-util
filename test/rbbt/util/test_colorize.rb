require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/colorize'
require 'test/unit'

class TestColorize < Test::Unit::TestCase
  def test_color_array
    a = (1..16).to_a

    assert_equal 16, Colorize.distinct(a).length
    assert_equal 16, Colorize.distinct(a).compact.uniq.length
  end

  def test_from_name
    assert Colorize.from_name('wheat') =~ /^#/
    assert Colorize.from_name('green') =~ /^#/
  end

  def test_color_continuous
    a = (1..16).to_a
    assert_equal Colorize.continuous(a).length, Colorize.continuous(a).uniq.length 
  end
end
