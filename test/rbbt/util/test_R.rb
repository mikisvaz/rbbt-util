require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/R'

class TestR < Test::Unit::TestCase
  def test_sum
    assert_equal "6", R.run('cat(3+3)').read
  end
end

