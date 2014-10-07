require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/matrix'

class TestClass < Test::Unit::TestCase
  def test_matrix
    tsv = TSV.open <<-EOF, :sep => " ", :type => :list
#X Y Z Type Case
a 2 3 v l
A 4 7 v L
m 2 3 c l
n 4 7 c L
    EOF
    tsv = TSV.open(tsv.to_s)

    melt =  tsv.matrix_melt("Letter", "Value", "Type", "Case")
    assert_equal "L", melt["A~Y"][2]
  end
end

