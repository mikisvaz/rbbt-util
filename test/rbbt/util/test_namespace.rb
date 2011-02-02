require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/namespace'
require 'rbbt/sources/organism'

class TestNamespace < Test::Unit::TestCase
  def test_identifier_files
    namespace = "Organism::Hsa"
    namespace.extend NameSpace
  end
end

