require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class TestTSVParallel < Test::Unit::TestCase

  def test_pthrough
    require 'rbbt/sources/organism'
    tsv = Organism.identifiers("Hsa").tsv :unnamed => true

    h = {}
    tsv.monitor = true
    tsv.pthrough do |k,v|
      h[k] = v.first
    end

    assert h.size > 0
  end

end
