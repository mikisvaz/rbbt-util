require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/format'

class TestClass < Test::Unit::TestCase
  def test_fixutf8
    a = "Camión"
    assert_equal a, Misc.fixutf8(a)
  end
end

