require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R/eval'

class TestREval < Test::Unit::TestCase
  def test_get
    Misc.benchmark(100) do
      a = R.eval <<-EOF
    p = 12
    a = p * 2
    c(a)
      EOF
      assert_equal 12 * 2, a
    end
  end
end

