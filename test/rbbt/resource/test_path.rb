require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/resource/path'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestTSV < Test::Unit::TestCase

  def test_prev
    path = Path.setup "/tmp"
    assert_equal "/tmp/bar/foo", path.foo("bar")
  end
end
