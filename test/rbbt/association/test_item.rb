$LOAD_PATH.unshift(File.join(File.dirname(__FILE__), '../../..', 'lib'))
$LOAD_PATH.unshift(File.dirname(__FILE__))
require 'test/unit'

require 'scout/association'
class TestAssociationItem < Test::Unit::TestCase
  def test_incidence
    pairs = [[:A, :a], [:B, :b]].collect{|p| "#{p.first.to_s}~#{p.last.to_s}"}
    assert TSV === AssociationItem.incidence(pairs)
    assert_equal 2, AssociationItem.incidence(pairs).length
    assert_equal 2, AssociationItem.incidence(pairs).fields.length
  end
end

