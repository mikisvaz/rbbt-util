require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R/eval'

class TestREval < Test::Unit::TestCase
  def test_get
    Misc.benchmark(1000) do
      a = R.eval <<-EOF
    p = 12
    a = p * 2
    c(a)
      EOF
      assert_equal 12 * 2, a
    end
  end

  def test_TSV_fork
    tsv = TSV.setup({"1" => "1"},:type => :single)
      a = tsv.R <<-EOF, :R_method => :eval
    p = 12
    a = p * 2
    c(a)
      EOF

      pid = Process.fork do
        a = tsv.R <<-EOF, :R_method => :eval
    p = 12
    a = p * 2
    c(a)
        EOF
      end
      s = Process.waitpid2 pid
  end

  def test_TSV
    tsv = TSV.setup({"1" => "1"},:type => :single)
      a = tsv.R <<-EOF, :R_method => :eval
    p = 12
    a = p * 2
    c(a)
      EOF
  end
end

