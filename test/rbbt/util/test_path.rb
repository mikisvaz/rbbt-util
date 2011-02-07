require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt'
require 'rbbt/util/path'

class TestPath < Test::Unit::TestCase
  def test_namespace
    assert_equal nil, Rbbt.files.foo.namespace
  end
end

