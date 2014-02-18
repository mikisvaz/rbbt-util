require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/simpleopt/parse'

class TestSOPTParse < Test::Unit::TestCase
  def test_parse
    SOPT.parse("-f--first* first arg:-f--fun")
    assert_equal "fun", SOPT.shortcuts["fu"]
  end
end

