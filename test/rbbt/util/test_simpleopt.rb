require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/simpleopt'
require 'test/unit'

class TestSOPT < Test::Unit::TestCase

  def test_cmd_option_string
    SOPT.parse("-h--help:-to--tsv-options")
    assert_equal "tsv-options", SOPT.shortcuts["to"]
  end
end
