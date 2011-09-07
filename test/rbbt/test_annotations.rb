require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

module AnnotatedString
  extend Annotation
  self.annotation :annotation_str

  def add_annot
    self + annotation_str
  end
end

module AnnotatedString2
  extend Annotation
  include AnnotatedString
  self.annotation :annotation_str2
end

class TestAnnotations < Test::Unit::TestCase

  def test_annotated_string
    assert_equal %w(annotation_str), AnnotatedString.annotations.collect{|a| a.to_s}
  end

  def test_string
    str = "string"
    annotation_str = "Annotation String"
    AnnotatedString.setup(str, annotation_str)
    assert_equal [AnnotatedString], str.annotation_types
    assert_equal annotation_str, str.annotation_str
  end

  def test_array
    ary = ["string"]
    annotation_str = "Annotation String"
    AnnotatedArray.setup_chain(ary)
    AnnotatedString.setup(ary, annotation_str)
    assert_equal [AnnotatedString], ary.annotation_types
    assert_equal annotation_str, ary.annotation_str
    assert_equal annotation_str, ary[0].annotation_str
  end

  def test_info
    ary = ["string"]
    annotation_str = "Annotation String"
    AnnotatedString.setup(ary, annotation_str)
    assert_equal({:annotation_str => annotation_str, :annotation_types => [AnnotatedString]}, ary.info)
  end

  def test_load
    str = "string"
    annotation_str = "Annotation String"
    info = {:annotation_str => annotation_str, :annotation_types => [AnnotatedString]}

    Annotated.load(str, info)
    assert_equal annotation_str, str.annotation_str
  end

  def test_tsv
    str1 = "string1"
    annotation_str1 = "Annotation String 1"
    str2 = "string2"
    annotation_str2 = "Annotation String 2"
    AnnotatedString.setup(str1, annotation_str1)
    AnnotatedString.setup(str2, annotation_str2)
    assert_equal str1, Annotated.tsv([str1, str2], :all)[str1.id]["literal"] 
    assert_equal annotation_str1, Annotated.tsv([str1, str2], :annotation_str, :JSON)[str1.id]["annotation_str"] 
  end

  def test_literal
    str = "string"
    annotation_str = "Annotation String"
    AnnotatedString.setup(str, annotation_str)
    assert_equal ["string"], str.tsv_values("literal")
  end

  def test_load_tsv
    str1 = "string1"
    annotation_str1 = "Annotation String 1"
    str2 = "string2"
    annotation_str2 = "Annotation String 2"
    AnnotatedString.setup(str1, annotation_str1)
    AnnotatedString.setup(str2, annotation_str2)
    assert_equal annotation_str1, Annotated.load_tsv(Annotated.tsv([str1, str2], :all)).sort.first.annotation_str
    assert_equal str1, Annotated.load_tsv(Annotated.tsv([str1, str2], :literal, :JSON)).sort.first
  end

  def test_inheritance
    str = "string1"
    annotation_str1 = "Annotation String 1"
    annotation_str2 = "Annotation String 2"
    assert_equal [AnnotatedString], AnnotatedString2.inheritance
    AnnotatedString2.setup(str, annotation_str1, annotation_str2)
    assert_equal annotation_str1, str.annotation_str
    assert_equal annotation_str2, str.annotation_str2
  end

  def test_annotation_methods
    str = "string"
    annotation_str = "Annotation String"
    AnnotatedString.setup(str, annotation_str)
    assert_equal str + annotation_str, str.add_annot
  end
end
