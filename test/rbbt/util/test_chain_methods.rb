require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/chain_methods'
require 'test/unit'

module TestChainedModule
  extend ChainMethods

  def test_chain_get_brackets(value)
    self.test_chain_clean_get_brackets(value).reverse
  end

  self.chain_prefix = :test_chain
end

class TestChaimMethods < Test::Unit::TestCase
  def test_chained_reverse_get
    a = ["test", "TEST"]
    a.extend TestChainedModule
    assert_equal "test".reverse, a[0]
    assert_equal "TEST".reverse, a[1]
  end
end
