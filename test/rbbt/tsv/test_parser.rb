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
        parser = TSV::Parser.new(io)
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
        k = k.first if Array === k
        assert_equal "row1", k
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
        k = k.first if Array === k
        assert_equal "row1", k
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
        parser = TSV::Parser.new(io, :type => :double)
        parser.traverse do |k,v,f|
          assert_equal f, %w(Value)
        end
      end
    end
  end

  def test_merge
    content =<<-EOF
#: :type=:double
#PMID:Sentence number:TF:TG	Transcription Factor (Associated Gene Name)	Target Gene (Associated Gene Name)	Sign	Negation	PMID
24265317:3:NR1H3:FASN	NR1H3	FASN			24265317
17522048:0:NR1H3:FASN	NR1H3	FASN	+		17522048
19903962:0:NR1H3:FASN	NR1H3	FASN			19903962
19903962:7:NR1H3:FASN	NR1H3	FASN			19903962
22183856:4:NR1H3:FASN	NR1H3	FASN			22183856
22641099:4:NR1H3:FASN	NR1H3	FASN	+		22641099
23499676:8:NR1H3:FASN	NR1H3	FASN	+		23499676
11790787:5:NR1H3:FASN	NR1H3	FASN			11790787
11790787:7:NR1H3:FASN	NR1H3	FASN	+		11790787
11790787:9:NR1H3:FASN	NR1H3	FASN	+		11790787
11790787:11:NR1H3:FASN	NR1H3	FASN			11790787
17522048:1:NR1H3:FASN	NR1H3	FASN	+		17522048
17522048:3:NR1H3:FASN	NR1H3	FASN			17522048
22160584:1:NR1H3:FASN	NR1H3	FASN			22160584
22160584:5:NR1H3:FASN	NR1H3	FASN	+		22160584
22160584:8:NR1H3:FASN	NR1H3	FASN	+		22160584
    EOF

    TmpFile.with_file(content) do |filename|
      tsv = TSV.open(filename, :key_field => "Transcription Factor (Associated Gene Name)", :fields => ["Target Gene (Associated Gene Name)", "Sign", "PMID"], :merge => true, :type => :double)
      assert_equal 16, tsv["NR1H3"]["Sign"].length
    end
  end

end

