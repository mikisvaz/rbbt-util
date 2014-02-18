require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/simpleopt/parse'
require 'rbbt/util/simpleopt/get'

class TestSOPTParse < Test::Unit::TestCase
  def test_consume
    SOPT.parse("-f--first* first arg:-f--fun")
    args = "-f myfile --fun".split(" ")
    assert_equal "myfile", SOPT.consume(args)[:first]
  end
end

