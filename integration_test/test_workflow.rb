require File.join(File.expand_path(File.dirname(__FILE__)), 'test_helper.rb')
require 'rbbt-util'

class TestWorkflow < Test::Unit::TestCase

  def test_path
    assert TSV === Rbbt.files.Organism.Hsa.identifiers.tsv
    assert Rbbt.files.Polysearch.location.index["cilium"].include? "SL00015"
  end
end

