require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/R'

class TestR < Test::Unit::TestCase
  def test_sum
    assert_equal "6", R.run('cat(3+3)').read.split(/\n/).last
  end

  def test_tsv_R
    tsv = TSV.setup({:a => 1, :b => 2})
    tsv2 = tsv.R <<-EOF
data = data + 1
    EOF
    assert_equal "2", tsv2["a"].first
  end

  def test_format_tsv
    tsv = TSV.setup({"a" => [1], "b" => [2]}, :type => :list, :key_field => "Letter", :fields => ["Number"])
    assert tsv.transpose("Field").include? "Number"
  end

  def test_error
    assert_raise ProcessFailed do
      R.run <<-EOF
        fadsfasdf
      EOF
    end
  end

  def test_hash2Rargs
    hash = {:verbose => false, :iter => 1000, :method => 'kmeans'}
    str = R.hash2Rargs(hash)
    assert_equal "verbose=FALSE, iter=1000, method='kmeans'", str
  end
end

