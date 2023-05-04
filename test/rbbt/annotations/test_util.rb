require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/entity'
require 'rbbt/annotations'

module TestEntityString
  extend Entity
  self.annotation :code
end

class TestAnnotation < Test::Unit::TestCase
  def test_marshal
    a = "STRING"
    a = TestEntityString.setup(a)

    #assert !(Annotated === Marshal.load(Marshal.dump(a)))
    assert_equal a, Marshal.load(Marshal.dump(a))
  end

  def test_marshal_array
    a = ["STRING"]
    TestEntityString.setup(a)

    #assert !(Annotated === Marshal.load(Marshal.dump(a)))
    assert Annotated === a
    assert Annotated === a.first
    assert_equal a, Marshal.load(Marshal.dump(a))
  end

  def test_hash
    e = TestEntityString.setup("TEST", :code => 10)
    assert_equal "TEST", Annotated.to_hash(e)[:literal]
    assert_equal 10, Annotated.to_hash(e)[:info][:code]
  end

  def test_traverse_cpus
    strings = (1..10).to_a.collect{|n| n.to_s }
    res = TSV.traverse strings, :cpus => 2, :into => [] do |s|
      TestEntityString.setup(s, :code => s.to_i)
    end

    assert TestEntityString === res.first
  end
end

