require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/annotations'

module TestEntityString
  extend Entity
  self.annotation :code
end

class TestAnnotation < Test::Unit::TestCase
  def test_marshal
    a = "STRING"
    a.extend Annotated

    assert !(Annotated === Marshal.load(Marshal.dump(a)))
    assert_equal a, Marshal.load(Marshal.dump(a))
  end

  def test_hash
    e = TestEntityString.setup("TEST", :code => 10)
    assert_equal "TEST", Annotated.to_hash(e)[:literal]
    assert_equal 10, Annotated.to_hash(e)[:info][:code]
  end
end

