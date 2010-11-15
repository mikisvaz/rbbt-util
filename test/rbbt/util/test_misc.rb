require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def test_string2hash
    assert(Misc.string2hash("--user-agent=>firefox").include? "--user-agent")
  end
  
  def test_named_array
    a = NamedArray.name([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

end
