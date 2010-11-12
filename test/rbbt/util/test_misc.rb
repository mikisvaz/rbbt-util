require File.dirname(__FILE__) + '/../../test_helper'
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def test_string2hash
    assert(Misc.string2hash("--user-agent=>firefox").include? "--user-agent")
  end

end
