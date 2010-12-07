require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/excel2tsv'

class TestTSV < Test::Unit::TestCase
  def test_tsv2excel
    tsv = TSV.excel2tsv(test_datafile('Test.xls'), :header => true)
    assert_equal 'Id', tsv.key_field
  end
end

