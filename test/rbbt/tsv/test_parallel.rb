require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class TestTSVParallel < Test::Unit::TestCase

  def _test_pthrough
    require 'rbbt/sources/organism'
    tsv = Organism.identifiers("Hsa").tsv :unnamed => true, :persist => true

    h = {}
    tsv.monitor = true
    tsv.unnamed = true
    Misc.benchmark do
    tsv.pthrough do |k,v|
      h[k] = v.first
    end
    end

    assert_equal tsv.size, h.size
    assert_equal tsv.keys.sort, h.keys.sort
  end


  def test_ppthrough
    require 'rbbt/sources/organism'
    tsv = Organism.identifiers("Hsa").tsv :unnamed => true, :persist => false, :fields => ["Associated Gene Name"]

    h = {}

    tsv.ppthrough_callback do |k,v|
      h[k] = v
    end

    tsv.unnamed = true
    tsv.monitor = true
    Misc.benchmark do
    tsv.ppthrough(3) do |k,v|
      [k,v.first]
    end
    end

    assert_equal tsv.size, h.size
    assert_equal tsv.keys.sort, h.keys.sort
  end
end
