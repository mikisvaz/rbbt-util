require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/colorize'
require 'test/unit'

class TestColorize < Test::Unit::TestCase
  def test_color_array
    a = [:red, :red, :blue, :blue, :yellow]
    a = (0..16).to_a

    ddd Colorize.distinct(a)
  end
end
