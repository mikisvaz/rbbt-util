require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class TestTSVParallelThrough < Test::Unit::TestCase

  def _test_pthrough
    tsv = datafile_test('identifiers').tsv :unnamed => true, :persist => false, :fields => ["Associated Gene Name"]

    h = {}
    tsv.monitor = true
    tsv.unnamed = true
    tsv.pthrough do |k,v|
      h[k] = v.first
    end
    raise

    assert_equal tsv.size, h.size
    assert_equal tsv.keys.sort, h.keys.sort
  end


  def _test_ppthrough
    tsv = datafile_test('identifiers').tsv :unnamed => true, :persist => false, :fields => ["Associated Gene Name"]

    h = {}

    tsv.ppthrough_callback do |k,v|
      h[k] = v
    end

    tsv.unnamed = true
    tsv.monitor = true
    tsv.ppthrough(3) do |k,v|
      [k,v.first]
    end

    assert_equal tsv.size, h.size
    assert_equal tsv.keys.sort, h.keys.sort
  end
end
