require File.expand_path(File.join(File.dirname(__FILE__), '../..', 'test_helper.rb'))
require 'rbbt/tsv'

class TestTSVParser < Test::Unit::TestCase
  def test_traverse

    content =<<-EOF
row1    a|aa|aaa    b    Id1|Id2
    EOF

    TmpFile.with_file(content) do |filename|
      TSV::Parser.traverse(Open.open(filename), :sep => /\s+/, :fields => [1], :type => :double) do |k,v|
        assert_equal ["row1"], k
        assert_equal [%w(a aa aaa)], v
      end

      TSV::Parser.traverse(Open.open(filename), :sep => /\s+/, :fields => [1], :type => :list) do |k,v|
        assert_equal "row1", k
        assert_equal ["a|aa|aaa"], v
      end

      TSV::Parser.traverse(Open.open(filename), :sep => /\s+/, :fields => [1], :type => :single) do |k,v|
        assert_equal "row1", k
        assert_equal "a|aa|aaa", v
      end

      TSV::Parser.traverse(Open.open(filename), :sep => /\s+/, :fields => [1], :type => :flat) do |k,v|
        assert_equal "row1", k
        assert_equal %w(a aa aaa), v
      end
    end
  end
end

