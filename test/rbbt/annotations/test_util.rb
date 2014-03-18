require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/annotations'

class TestClass < Test::Unit::TestCase
  def test_marshal
    a = "STRING"
    a.extend Annotated

    assert !(Annotated === Marshal.load(Marshal.dump(a)))
    assert_equal a, Marshal.load(Marshal.dump(a))
  end
end

