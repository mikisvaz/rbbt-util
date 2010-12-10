require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/simpleopt'
require 'test/unit'

class TestSOPT < Test::Unit::TestCase

  def test_cmd_option_string
    assert_equal("--tsv-options", SOPT.parse("-h--help:-to--tsv-options")["tsv-options"][:long])
  end
end
