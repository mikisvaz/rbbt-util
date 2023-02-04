require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/python'
require 'rbbt/util/python/util'

class TestPythonUtil < Test::Unit::TestCase

  def test_tuple
    tsv = TSV.setup([], :key_field => "Key", :fields => %w(Value1 Value2), :type => :list)
    tsv["k1"] = %w(V1_1 V2_1)
    tsv["k2"] = %w(V1_2 V2_2)
    df = RbbtPython.tsv2df(tsv)
    new_tsv = RbbtPython.df2tsv(df)
    assert_equal tsv, new_tsv
  end

  def test_numpy
    ra = RbbtPython.run :numpy, :as => :np do
      na = np.array([[[1,2,3], [4,5,6]]])
      RbbtPython.numpy2ruby na
    end
    assert_equal 6, ra[0][1][2]
  end

end

