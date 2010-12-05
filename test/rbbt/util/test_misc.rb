require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def test_string2hash
    assert(Misc.string2hash("--user-agent=firefox").include? "--user-agent")
    assert(Misc.string2hash(":true")[:true] == true)
    assert(Misc.string2hash("true")["true"] == true)
    assert(Misc.string2hash("a=1")["a"] == 1)
    assert(Misc.string2hash("a=b")["a"] == 'b')
    assert(Misc.string2hash("a=b#c=d#:h=j")["c"] == 'd')
    assert(Misc.string2hash("a=b#c=d#:h=j")[:h] == 'j')
  end
  
  def test_named_array
    a = NamedArray.name([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

end
