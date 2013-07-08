require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt'
require 'rbbt/entity'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestA
  attr_accessor :foo, :bar
  def initialize(foo, bar)
    @foo = foo
    @bar = bar
  end
end

module ReversableString
  extend Entity

  self.annotation :foo, :bar

  property :reverse_text_ary => :array do
    $count += 1
    self.collect{|s| s.reverse}
  end
 
  property :reverse_text_single => :single do
    $count += 1
    self.reverse
  end

  property :reverse_text_ary_p => :array do
    $count += 1
    self.collect{|s| s.reverse}
  end
 
  property :reverse_text_single_p => :single do
    $count += 1
    self.reverse
  end

  property :reverse_text_ary_p_array => :array do
    $count += 1
    self.collect{|s| s.reverse}
  end

  property :random => :single do
    rand
  end

  property :annotation_list => :single do
    self.chars.to_a.collect{|c| 
      ReversableString.setup(c)
    }
  end
 
  persist :reverse_text_ary_p
  persist :reverse_text_ary_p
  persist :reverse_text_ary_p
  persist :reverse_text_single_p

  persist :reverse_text_ary_p_array, :array, :dir => TmpFile.tmp_file

  persist :annotation_list, :annotations, :dir => TmpFile.tmp_file
end

class TestEntity < Test::Unit::TestCase

  def test_property_ary
    a = ["String1", "String2"]
    ReversableString.setup(a)

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_ary.last
    assert_equal 1, $count
    a._ary_property_cache.clear
    assert_equal "2gnirtS", a[1].reverse_text_ary
    assert_equal 2, $count
    a._ary_property_cache.clear

    $count = 0
    a.each do |string|
      string.reverse_text_ary
      assert_equal 1, $count
    end
  end

  def test_property_single
    a = ["String1", "String2"]
    ReversableString.setup a

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_single.last
    assert_equal 2, $count
    assert_equal "2gnirtS", a[1].reverse_text_single
    assert_equal 3, $count
  end

  def test_property_ary_p
    a = ["String1", "String2"]
    ReversableString.setup a

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_ary_p.last
    assert_equal "2gnirtS", a[1].reverse_text_ary_p
    assert_equal 1, $count
  end

  def test_property_single_p
    a = ["String1", "String2"]
    ReversableString.setup a

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_single_p.last
    assert_equal 2, $count
    assert_equal "2gnirtS", a[1].reverse_text_single_p
    assert_equal 2, $count
  end

  def test_property_ary_p_array
    a = ["String1", "String2"]
    ReversableString.setup a

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_ary_p_array.last
    assert_equal 1, $count
    assert_equal "2gnirtS", a.reverse_text_ary_p_array.last
    assert_equal 1, $count
  end

  def test_unpersist
    a = ["String1", "String2"]
    ReversableString.setup a

    # Before persist
    assert(! ReversableString.persisted?(:random))

    r1 = a.random
    r2 = a.random
    assert_not_equal r1, r2

    # After persist
    ReversableString.persist :random
    assert(ReversableString.persisted?(:random))

    r1 = a.random
    r2 = a.random
    assert_equal r1, r2

    # After unpersist
    ReversableString.unpersist :random
    assert(! ReversableString.persisted?(:random))

    r1 = a.random
    r2 = a.random
    assert_not_equal r1, r2

  end

  def test_persist_annotations
    string = 'aaabbbccc'
    ReversableString.setup(string)
    assert_equal string.length, string.annotation_list.length
    assert_equal string.length, string.annotation_list.length
  end

  def __test_performance
    require 'rbbt/entity/gene'
    Misc.profile(:min_percent => 2) do
      1000.times do
        TestA.new "foo", "bar"
      end
    end

    Misc.profile(:min_percent => 2) do
      1000.times do
        Gene.setup_positional("", "foo", "bar")
      end
    end

    Misc.benchmark(100000) do
      Gene.setup("", :foo => "foo", :bar => "bar")
    end

    Misc.benchmark(100000) do
      TestA.new "foo", "bar"
    end
  end

  def __test_clean_annotations
    Workflow.require_workflow "StudyExplorer"

    s = Study.setup("CLL")
    mutations = s.cohort.metagenotype

    mis = mutations.mutated_isoforms.compact.flatten

    Misc.profile(:min_percent => 1) do
      mis.each{|m| m}
    end


    Misc.benchmark(10) do
      mis.each{|m| m}
    end

    Misc.benchmark(10) do
      mis.clean_annotations.each{|m| m}
    end

    m = mutations.first

    assert_equal m.split(":")[1], m.position.to_s
    assert_raise NoMethodError do 
      m.clean_annotations.position
    end
  end
end
