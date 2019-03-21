require File.expand_path(File.join(File.dirname(__FILE__), '../..', 'test_helper.rb'))
require 'rbbt/tsv'

class TestTSVParser < Test::Unit::TestCase
  def test_flat
    TmpFile.with_file do |tmpdir|
      file = File.join(tmpdir, 'file')
      str =<<-EOF
#: :type=:flat#:sep=' '
#Key Value
a b c d e
A B C D E
      EOF
      Open.write(file, str)
      Open.open(file) do |io|
        parser = TSV::Parser.new(io, :merge => true, :zipped => true)
        parser.traverse do |k,v|
          assert v.length > 1
        end
      end
    end
  end

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
        assert_equal ["row1"], k
        assert_equal %w(a aa aaa), v
      end
    end
  end

  def test_fields
    TmpFile.with_file do |tmpdir|
      file = File.join(tmpdir, 'file')
      str =<<-EOF
#: :type=:flat#:sep=' '
#Key Value
a b c d e
A B C D E
      EOF
      Open.write(file, str)
      Open.open(file) do |io|
        parser = TSV::Parser.new(io, :merge => true, :zipped => true, :type => :double)
        parser.traverse do |k,v,f|
          assert_equal f, %w(Value)
        end
      end
    end
  end

end

