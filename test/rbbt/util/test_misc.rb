require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def _test_humanize
    assert_equal "mutation_enrichment", Misc.humanize("MutationEnrichment")
  end

  def _test_fixutf8
    string = "abc\xffdef"
    string = string.force_encoding("UTF-8") if string.respond_to? :force_encoding
    assert(! string.valid_encoding?) if string.respond_to? :valid_encoding?
    assert(! string.valid_encoding) if string.respond_to? :valid_encoding
    assert( Misc.fixutf8(string).valid_encoding?) if string.respond_to? :valid_encoding?
    assert( Misc.fixutf8(string).valid_encoding) if string.respond_to? :valid_encoding
  end

  def _test_colors_for
    colors, used = Misc.colors_for([1,2,2,1,2,1,2,2,3,3,2,3,2])
    assert_equal Misc::COLOR_LIST[1], used[2]
  end

  def _test_total_length
    ranges = [(0..100), (50..150), (120..160)]
    ranges = [(0..100), (50..150), (120..160), (51..70)]
    assert_equal 161, Misc.total_length(ranges)
  end

  def _test_id_filename?
    TmpFile.with_file("") do |file|
      assert Misc.is_filename?(file)
      assert ! Misc.is_filename?("TEST STRING")
    end
  end

  def _test_merge_sorted_arrays
    assert_equal [1,2,3,4], Misc.merge_sorted_arrays([1,3], [2,4])
  end

  def _test_intersect_sorted_arrays
    assert_equal [2,4], Misc.intersect_sorted_arrays([1,2,3,4], [2,4])
  end
  def _test_process_to_hash
    list = [1,2,3,4]
    assert_equal 4, Misc.process_to_hash(list){|l| l.collect{|e| e * 2}}[2]
  end

#  def _test_pdf2text_example
#    assert PDF2Text.pdf2text(datafile_test('example.pdf')).read =~ /An Example Paper/i
#  end
#
#  def _test_pdf2text_EPAR
#    assert PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB/document_library/EPAR_-_Scientific_Discussion/human/000402/WC500033103.pdf").read =~ /Tamiflu/i
#  end
#
#  def _test_pdf2text_wrong
#    assert_raise CMD::CMDError do PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB#").read end
#  end

  def _test_string2hash
    assert(Misc.string2hash("--user-agent=firefox").include? "--user-agent")
    assert_equal(true, Misc.string2hash(":true")[:true])
    assert_equal(true, Misc.string2hash("true")["true"])
    assert_equal(1, Misc.string2hash("a=1")["a"])
    assert_equal('b', Misc.string2hash("a=b")["a"])
    assert_equal('d', Misc.string2hash("a=b#c=d#:h=j")["c"])
    assert_equal('j', Misc.string2hash("a=b#c=d#:h=j")[:h])
    assert_equal(:j, Misc.string2hash("a=b#c=d#:h=:j")[:h])
  end
  
  def _test_named_array
    a = NamedArray.setup([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

#  def _test_path_relative_to
#    assert_equal "test/foo", Misc.path_relative_to('test/test/foo', 'test')
#  end

#  def _test_chunk
#    _test =<<-EOF
#This is an example file. Entries are separated by Entry
#-- Entry
#1
#2
#3
#-- Entry
#4
#5
#6
#    EOF
#
#    assert_equal "1\n2\n3", Misc.chunk(test, /^-- Entry/).first.strip
#  end

  def _test_hash2string
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

  def _test_merge
    a = [[1],[2]]
    a = NamedArray.setup a, %w(1 2)
    a.merge [3,4]
    assert_equal [1,3], a[0]
  end

  def _test_indiferent_hash
    a = {:a => 1, "b" => 2}
    a.extend IndiferentHash

    assert_equal 1, a["a"]
    assert_equal 1, a[:a]
    assert_equal 2, a["b"]
    assert_equal 2, a[:b]
  end

  def _test_lockfile

    TmpFile.with_file do |tmpfile|
      pids = []
      4.times do |i|
        pids << Process.fork do 
          pid = Process.pid().to_s
          status = Misc.lock(tmpfile, pid) do |f, val|
            Open.write(f, val)
            sleep rand * 2
            if pid == Open.read(tmpfile)
              0
            else
              1
            end
          end
          exit(status)
        end

      end
      pids.each do |pid| Process.waitpid pid; assert $?.success? end
    end
  end

  def _test_positions2hash
    inputs = Misc.positional2hash([:one, :two, :three], 1, :two => 2, :four => 4)
    assert_equal 1, inputs[:one]
    assert_equal 2, inputs[:two]
    assert_equal nil, inputs[:three]
    assert_equal nil, inputs[:four]
  end

  def _test_mean
    assert_equal 2, Misc.mean([1,2,3])
    assert_equal 3, Misc.mean([1,2,3,4,5])
  end

  def __test_sd
    assert_equal Math.sqrt(2), Misc.sd([1,3])
  end

  def _test_align_small
    reference = "AABCDEBD"
    sequence  = "ABCD"
    assert_equal '-ABCD---', Misc.fast_align(reference, sequence).last
  end

  def _test_align_real
    reference = "SGNECNKAIDGNKDTFWHTFYGANGDPKPPPHTYTIDMKTTQNVNGLSMLPRQDGNQNGWIGRHEVYLSSDGTNW"
    sequence  = "TYTIDMKTTQNVNGLSML"
    assert_equal "--------------------------------TYTIDMKTTQNVNGLSML-------------------------", Misc.fast_align(reference, sequence).last
  end

  def _test_divide
    assert_equal 2, Misc.divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def _test_ordered_divide
    assert_equal 5, Misc.ordered_divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def _test_collapse_ranges
    ranges = [(0..100), (50..150), (51..61),(200..250), (300..324),(320..350)]
    assert_equal [(0..150),(200..250), (300..350)], Misc.collapse_ranges(ranges)
  end

  def _test_humanize
    str1 = "test_string"
    str2 = "TEST_string"
    str3 = "test"

    assert_equal "Test string", Misc.humanize(str1)
    assert_equal "TEST string", Misc.humanize(str2)
    assert_equal "Test", Misc.humanize(str3)
  end

  def _test_snake_case
    str1 = "ACRONIMTest"
    str2 = "ACRONIM_test"
    assert_equal "ACRONIM_test", Misc.snake_case(str1)
    assert_equal "ACRONIM_test", Misc.snake_case(str2)
  end

  def _test_correct_vcf_mutations
    assert_equal [737407, ["-----", "-----G", "-----GTTAAT"]], Misc.correct_vcf_mutation(737406, "GTTAAT", "G,GG,GGTTAAT")
  end

  def test_fingerprint
    puts Misc.fingerprint({:a => 1})
  end
end
