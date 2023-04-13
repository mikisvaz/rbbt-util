require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestIndiferentHash < Test::Unit::TestCase
  def test_recursive
    a = {:a => {:b => 1 } }
    IndiferentHash.setup(a)

    assert IndiferentHash === a["a"]
    assert_equal 1, a[:a]["b"]
    assert_equal 1, a["a"][:b]
  end
end

