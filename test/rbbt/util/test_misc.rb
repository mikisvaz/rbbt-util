require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'

class TestMisc < Test::Unit::TestCase

  def test_humanize
    assert_equal "mutation_enrichment", Misc.humanize("MutationEnrichment")
  end

  def test_fixutf8
    string = "abc\xffdef"
    string = string.force_encoding("UTF-8") if string.respond_to? :force_encoding
    assert(! string.valid_encoding?) if string.respond_to? :valid_encoding?
    assert(! string.valid_encoding) if string.respond_to? :valid_encoding
    assert( Misc.fixutf8(string).valid_encoding?) if string.respond_to? :valid_encoding?
    assert( Misc.fixutf8(string).valid_encoding) if string.respond_to? :valid_encoding
  end

  def test_colors_for
    colors, used = Misc.colors_for([1,2,2,1,2,1,2,2,3,3,2,3,2])
    assert_equal Misc::COLOR_LIST[1], used[2]
  end

  def test_total_length
    ranges = [(0..100), (50..150), (120..160)]
    ranges = [(0..100), (50..150), (120..160), (51..70)]
    assert_equal 161, Misc.total_length(ranges)
  end

  def test_id_filename?
    TmpFile.with_file("") do |file|
      assert Misc.is_filename?(file)
      assert ! Misc.is_filename?("TEST STRING")
    end
  end

  def test_merge_sorted_arrays
    assert_equal [1,2,3,4], Misc.merge_sorted_arrays([1,3], [2,4])
  end

  def test_intersect_sorted_arrays
    assert_equal [2,4], Misc.intersect_sorted_arrays([1,2,3,4], [2,4])
  end
  def test_process_to_hash
    list = [1,2,3,4]
    assert_equal 4, Misc.process_to_hash(list){|l| l.collect{|e| e * 2}}[2]
  end

#  def test_pdf2text_example
#    assert PDF2Text.pdf2text(datafile_test('example.pdf')).read =~ /An Example Paper/i
#  end
#
#  def test_pdf2text_EPAR
#    assert PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB/document_library/EPAR_-_Scientific_Discussion/human/000402/WC500033103.pdf").read =~ /Tamiflu/i
#  end
#
#  def test_pdf2text_wrong
#    assert_raise CMD::CMDError do PDF2Text.pdf2text("http://www.ema.europa.eu/docs/en_GB#").read end
#  end

  def test_string2hash
    assert(Misc.string2hash("--user-agent=firefox").include? "--user-agent")
    assert_equal(true, Misc.string2hash(":true")[:true])
    assert_equal(true, Misc.string2hash("true")["true"])
    assert_equal(1, Misc.string2hash("a=1")["a"])
    assert_equal('b', Misc.string2hash("a=b")["a"])
    assert_equal('d', Misc.string2hash("a=b#c=d#:h=j")["c"])
    assert_equal('j', Misc.string2hash("a=b#c=d#:h=j")[:h])
    assert_equal(:j, Misc.string2hash("a=b#c=d#:h=:j")[:h])
  end
  
  def test_named_array
    a = NamedArray.setup([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

#  def test_path_relative_to
#    assert_equal "test/foo", Misc.path_relative_to('test/test/foo', 'test')
#  end

#  def test_chunk
#    test =<<-EOF
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

  def test_merge
    a = [[1],[2]]
    a = NamedArray.setup a, %w(1 2)
    a.merge [3,4]
    assert_equal [1,3], a[0]
  end

  def test_indiferent_hash
    a = {:a => 1, "b" => 2}
    a.extend IndiferentHash

    assert_equal 1, a["a"]
    assert_equal 1, a[:a]
    assert_equal 2, a["b"]
    assert_equal 2, a[:b]
  end

  def test_lockfile

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

  def test_positions2hash
    inputs = Misc.positional2hash([:one, :two, :three], 1, :two => 2, :four => 4)
    assert_equal 1, inputs[:one]
    assert_equal 2, inputs[:two]
    assert_equal nil, inputs[:three]
    assert_equal nil, inputs[:four]
  end

  def test_mean
    assert_equal 2, Misc.mean([1,2,3])
    assert_equal 3, Misc.mean([1,2,3,4,5])
  end

  def __test_sd
    assert_equal Math.sqrt(2), Misc.sd([1,3])
  end

  def test_align_small
    reference = "AABCDEBD"
    sequence  = "ABCD"
    assert_equal '-ABCD---', Misc.fast_align(reference, sequence).last
  end

  def test_align_real
    reference = "SGNECNKAIDGNKDTFWHTFYGANGDPKPPPHTYTIDMKTTQNVNGLSMLPRQDGNQNGWIGRHEVYLSSDGTNW"
    sequence  = "TYTIDMKTTQNVNGLSML"
    assert_equal "--------------------------------TYTIDMKTTQNVNGLSML-------------------------", Misc.fast_align(reference, sequence).last
  end

  def test_divide
    assert_equal 2, Misc.divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def test_ordered_divide
    assert_equal 5, Misc.ordered_divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def test_setup
    require 'rbbt/entity/gene'
    g = Misc.prepare_entity("TP53", "Gene", :format => "Associated Gene Name", "organism" => "Hsa/jun2011")
  end

#
#  def test_process_to_hash
#    list = [1,2,3,4]
#    assert_equal 4, Misc.process_to_hash(list){|l| l.collect{|e| e * 2}}[2]
#  end

#  def test_add_method
#    a = "Test"
#    Misc.add_method a, :invert do self.reverse end
#    assert_equal "Test".reverse, a.invert
#  end
#
#  def test_redefine_method
#    a = "Test"
#    worked = false
#    Misc.redefine_method a, :reverse, :old_reverse do worked = true; self.old_reverse end
#    assert_equal "Test".reverse, a.reverse
#    assert worked
#  end
#
#  def test_merge_sorted_arrays
#    assert_equal [1,2,3,4], Misc.merge_sorted_arrays([1,3], [2,4])
#  end
#
#  def test_intersect_sorted_arrays
#    assert_equal [2,4], Misc.intersect_sorted_arrays([1,2,3,4], [2,4])
#  end
#
#
#  def test_in_dir
#    TmpFile.with_file do |dir|
#      FileUtils.mkdir_p dir
#      Open.write(File.join(dir, 'test_file_in_dir'), 'test_file_in_dir')
#      Misc.in_dir(dir) do
#        assert Dir.glob("*").include? 'test_file_in_dir'
#      end
#      assert Dir.glob(File.join(dir, "*")).include?(File.join(dir, 'test_file_in_dir'))
#      assert(! Dir.glob("*").include?('test_file_in_dir'))
#    end
#  end

end
