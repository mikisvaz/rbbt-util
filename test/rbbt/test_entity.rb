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

Entity.entity_property_cache = Rbbt.tmp.test.entity_property.find
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

  property :reverse_text_ary_p => :array2single do
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

  persist :reverse_text_ary_p, :marshal
  persist :reverse_text_single_p, :memory

  persist :reverse_text_ary_p_array, :array, :dir => TmpFile.tmp_file

  persist :annotation_list, :annotations, :dir => TmpFile.tmp_file


  $processed_multiple = []
  property :multiple_annotation_list => :multiple do |list|
    $processed_multiple.concat list
    res = {}
    list.collect do |e|
      e.chars.to_a.collect{|c| 
        ReversableString.setup(c)
      }
    end
  end
  persist :multiple_annotation_list, :annotations, :dir => TmpFile.tmp_file
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

    a.reverse_text_ary_p

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_ary_p.last
    assert_equal "2gnirtS", a.collect{|e| e.reverse_text_ary_p }[1]
    assert_equal 0, $count
  end

  def test_property_single_p
    a = ["String1", "String2"]
    ReversableString.setup a

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_single_p.last

    assert_equal 2, $count

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_single_p.last
    assert_equal 0, $count
    assert_equal "2gnirtS", a[1].reverse_text_single_p
    assert_equal 1, $count
  end

  def test_property_ary_p_array
    a = ["String1", "String2"]
    ReversableString.setup a

    assert_equal "2gnirtS", a.reverse_text_ary_p_array.last

    $count = 0

    assert_equal "2gnirtS", a.reverse_text_ary_p_array.last
    assert_equal 0, $count
    assert_equal "2gnirtS", a.reverse_text_ary_p_array.last
    assert_equal 0, $count
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

  def test_persist_multiple_annotations
    string1 = 'aaabbbccc'
    string2 = 'AAABBBCCC'
    string3 = 'AAABBBCCC_3'
    string4 = 'AAABBBCCC_4'

    array = ReversableString.setup([string1, string2])
    assert_equal [string1, string2].collect{|s| s.chars}, array.multiple_annotation_list

    assert_equal string1.length, array[0].multiple_annotation_list.length
    assert_equal $processed_multiple, [string1, string2]

    array = ReversableString.setup([string2, string3])
    assert_equal string2.length, array[0].multiple_annotation_list.length
    assert_equal $processed_multiple, [string1, string2, string3]

    array = ReversableString.setup([string2, string3])
    assert_equal string2.length, array[0].multiple_annotation_list.length
    assert_equal $processed_multiple, [string1, string2, string3]

    $processed_multiple = []
    array = ReversableString.setup([string2, string3, string4])
    assert_equal string2.length, array[0].multiple_annotation_list.length
    assert_equal $processed_multiple, [string4]

    string1 = 'aaabbbccc'
    string2 = 'AAABBBCCC'
    string3 = 'AAABBBCCC_3'
    string4 = 'AAABBBCCC_4'
    $processed_multiple = []
    array = ReversableString.setup([string2, string3, string4])
    assert_equal string2.length, array[0].multiple_annotation_list.length
    assert_equal $processed_multiple, [string4]

  end

  def test_clean_annotations

    string = "test_string"
    ReversableString.setup string
    assert string.respond_to?(:reverse_text_single)
    assert ! string.clean_annotations.respond_to?(:reverse_text_single)

  end

  def test_all_properties
    assert ReversableString.setup("TEST").all_properties.include?("reverse_text_ary")
    assert_equal ReversableString.setup("TEST").all_properties, ReversableString.all_properties
  end
end
