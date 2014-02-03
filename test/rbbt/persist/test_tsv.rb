require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestAnnotation
  extend Annotation

  self.annotation :test_annotation
end

class TestPersistTSV < Test::Unit::TestCase
end
