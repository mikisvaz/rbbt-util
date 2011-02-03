require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def test_pdf2text_example
    assert PDF2Text.pdf2text(test_datafile('example.pdf')).read =~ /An Example Paper/i
  end

  def test_pdf2text_EPAR
    assert PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB/document_library/EPAR_-_Scientific_Discussion/human/000402/WC500033103.pdf").read =~ /Tamiflu/i
  end

  def test_pdf2text_wrong
    assert_raise CMD::CMDError do PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB#") end
  end

  def test_string2hash
    assert(Misc.string2hash("--user-agent=firefox").include? "--user-agent")
    assert(Misc.string2hash(":true")[:true] == true)
    assert(Misc.string2hash("true")["true"] == true)
    assert(Misc.string2hash("a=1")["a"] == 1)
    assert(Misc.string2hash("a=b")["a"] == 'b')
    assert(Misc.string2hash("a=b#c=d#:h=j")["c"] == 'd')
    assert(Misc.string2hash("a=b#c=d#:h=j")[:h] == 'j')
    assert(Misc.string2hash("a=b#c=d#:h=:j")[:h] == :j)
  end
  
  def test_named_array
    a = NamedArray.name([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

  def test_path_relative_to
    assert_equal "test/foo", Misc.path_relative_to('test/test/foo', 'test')
  end

  def test_chunk
    test =<<-EOF
This is an example file. Entries are separated by Entry
-- Entry
1
2
3
-- Entry
4
5
6
    EOF

    assert_equal "1\n2\n3", Misc.chunk(test, /^-- Entry/).first.strip
  end

  def test_hash2string
    hash = {}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))

    hash = {:a => 1}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))
 
    hash = {:a => true}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))

    hash = {:a => Misc}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))
 
    hash = {:a => :b}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))
 
    hash = {:a => /test/}
    assert_equal({}, Misc.string2hash(Misc.hash2string(hash)))
 


 end

end
