require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/misc'
require 'test/unit'
require 'rbbt/tsv'
require 'rbbt/entity'

class TestMisc < Test::Unit::TestCase

  def test_object_delta
    a = []
    b = nil
    d = Misc.object_delta(String) do
      a << rand.to_s
    end
    assert_equal 1, a.length
    assert_match /^0\.\d+$/, a.first
  end

  def test_format_seconds
    t = 61.3232
    assert_equal "00:01:01", Misc.format_seconds(t)
    assert_equal "00:01:01.32", Misc.format_seconds(t, true)
  end

  def test_format_paragraph
    p = <<-EOF
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.

Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt
in culpa qui officia deserunt mollit anim id est laborum.


    * one
    * two
    * three

    EOF

    assert Misc.format_paragraph(p, 70, 10, 5) =~ /\n\s*\* two/sm
  end

  def test_format_dl
    p1 = <<-EOF
Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor
incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis
nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.
Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu
    fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt
    in culpa qui officia deserunt mollit anim id est laborum.
    EOF

    p2 = <<-EOF

Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium
doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore
veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim
ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia
consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.

Neque porro quisquam est, qui dolorem ipsum quia dolor sit amet, consectetur,
adipisci velit, sed quia non numquam eius modi tempora incidunt ut labore et
dolore magnam aliquam quaerat voluptatem. Ut enim ad minima veniam, quis
nostrum exercitationem ullam corporis suscipit laboriosam, nisi ut aliquid ex
ea commodi consequatur? Quis autem vel eum iure reprehenderit qui in ea
voluptate velit esse quam nihil molestiae consequatur, vel illum qui dolorem
eum fugiat quo voluptas nulla pariatur?"
    EOF

    assert Misc.format_definition_list({:paragraph_first => p1, :paragraph_second => p2}) =~ /       /
  end

  def test_parse_cmd_params
    assert_equal ["workflow", "task", "Translation", "translate", "-f", "Associated Gene Name", "-l", "-"],
      Misc.parse_cmd_params("workflow task Translation translate -f 'Associated Gene Name' -l -")
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

  def test_sorted_array_matches
    assert_equal [1,3], Misc.sorted_array_hits(%w(a b c d e), %w(b d))
  end

  def test_binary_include?
    a = %w(a b c d e).sort
    assert Misc.binary_include?(a, "a")
    assert(!Misc.binary_include?(a, "z"))
    assert(Misc.binary_include?(a, "b"))
    assert(Misc.binary_include?(a, "c"))
    assert(Misc.binary_include?(a, "d"))
  end

  def test_process_to_hash
    list = [1,2,3,4]
    assert_equal 4, Misc.process_to_hash(list){|l| l.collect{|e| e * 2}}[2]
  end

  def test_pipe_fork
    sout, sin = Misc.pipe
    pid = Process.fork do
      Misc.purge_pipes(sin)
      sleep 2
      sin.close
    end
    sin.close
    assert sout.eof?
    Process.kill :INT, pid
  end

  def test_open_pipe
    t = 5
    stream = Misc.open_pipe do |sin|
      t.times do |i|
        sleep 0.5
        sin.puts "LINE #{ i }"
      end
    end

    time = Time.now
    lines = []
    while line = stream.gets
      lines << line.strip
    end
    time_spent = Time.new - time

    assert time_spent >= t * 0.5
    assert time_spent <= (t+1) * 0.5
    assert_equal (0..t-1).to_a.collect{|i| "LINE #{ i }"}, lines
  end

  def test_open_pipe_fork
    t = 5
    stream = Misc.open_pipe(true) do |sin|
      t.times do |i|
        sleep 0.5
        sin.puts "LINE #{ i }"
      end
    end

    time = Time.now
    lines = []
    while line = stream.gets
      lines << line.strip
    end
    time_spent = Time.new - time

    assert time_spent >= t * 0.5
    assert time_spent <= (t+1) * 0.5
    assert_equal (0..t-1).to_a.collect{|i| "LINE #{ i }"}, lines
  end

  def test_open_pipe_fork_cascade
    t = 500
    sleep_time = 2.0 / t
    time = Time.now

    stream1 = Misc.open_pipe(true) do |sin|
      t.times do |i|
        sleep sleep_time
        sin.puts "LINE #{ i }"
      end
    end

    stream2 = Misc.open_pipe(true) do |sin|
      while line = stream1.gets
        sin.puts line.strip.reverse
      end
    end

    stream3 = Misc.open_pipe(true) do |sin|
      while line = stream2.gets
        sin.puts line.downcase
      end
    end

    lines = []
    while line = stream3.gets
      lines << line.strip
    end

    time_spent = Time.new - time

    assert time_spent >= t * sleep_time
    assert time_spent <= t * 1.2 * sleep_time
    assert_equal (0..t-1).to_a.collect{|i| "LINE #{ i }".reverse.downcase}, lines
  end

  def test_tee_stream
    t = 500
    sleep_time = 2.0 / t
    time = Time.now

    stream1 = Misc.open_pipe(true) do |sin|
      t.times do |i|
        sleep sleep_time
        sin.puts "LINE #{ i }"
      end
    end

    stream2, stream3 = Misc.tee_stream stream1

    stream4 = Misc.open_pipe(true) do |sin|
      while line = stream2.gets
        sin.puts line.strip.reverse
      end
    end

    stream5 = Misc.open_pipe(true) do |sin|
      while line = stream3.gets
        sin.puts line.strip.downcase
      end
    end

    lines1 = []
    th1 = Thread.new do
      while line = stream4.gets
        lines1 << line.strip
      end
    end

    lines2 = []
    th2 = Thread.new do
      while line = stream5.gets
        lines2 << line.strip
      end
    end
    th1.join and th2.join

    time_spent = Time.new - time

    assert time_spent >= t * sleep_time
    assert time_spent <= t * 1.5 * sleep_time
    assert_equal (0..t-1).to_a.collect{|i| "LINE #{ i }".reverse}, lines1
    assert_equal (0..t-1).to_a.collect{|i| "LINE #{ i }".downcase}, lines2
  end


  def test_string2hash
    assert(Misc.string2hash("--user-agent=firefox").include? "--user-agent")
    assert_equal(true, Misc.string2hash(":true")[:true])
    assert_equal(true, Misc.string2hash("true")["true"])
    assert_equal(1, Misc.string2hash("a=1")["a"])
    assert_equal('b', Misc.string2hash("a=b")["a"])
    assert_equal('d', Misc.string2hash("a=b#c=d#:h='j'")["c"])
    assert_equal('j', Misc.string2hash("a=b#c=d#:h='j'")[:h])
    assert_equal(:j, Misc.string2hash("a=b#c=d#:h=:j")[:h])
  end

  def test_named_array
    a = NamedArray.setup([1,2,3,4], %w(a b c d))
    assert_equal(1, a['a'])
  end

  def test_path_relative_to
    assert_equal "test/foo", Misc.path_relative_to('/test', '/test/test/foo')
  end

  def test_hash2string
    hash = {}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))

    hash = {:a => 1}
    assert_equal hash, Misc.string2hash(Misc.hash2string(hash))

    hash = {:a => true}
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

    assert_equal 1, a[:a]
    assert_equal 1, a["a"]
    assert_equal 2, a["b"]
    assert_equal 2, a[:b]
  end

  def test_lockfile

    TmpFile.with_file do |tmpfile|
      pids = []
      4.times do |i|
        pids << Process.fork do
          status = Misc.lock(tmpfile) do
            pid = Process.pid.to_s
            Open.write(tmpfile, pid)
            sleep rand * 1
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

  def test_zip_fields
    current = [[:a,1], [:b,2]]
    assert_equal [[:a, :b],[1,2]], Misc.zip_fields(current)
    assert_equal current, Misc.zip_fields(Misc.zip_fields(current))
  end

  def test_zip_fields_comp
    current = [[:a,1], [:b,2], [:c]]
    assert_equal [[:a, :b, :c],[1,2,nil]], Misc.zip_fields(current)
    assert_equal current, Misc.zip_fields(Misc.zip_fields(current)).collect{|v| v.compact }
  end

  def test_add_zipped
    current = [[:a,1], [:b,2]]
    new = %w(A B)
    Misc.append_zipped current, new
    assert_equal [[:a,1,"A"], [:b,2,"B"]], current
  end

  def test_divide
    assert_equal 2, Misc.divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def test_ordered_divide
    assert_equal 5, Misc.ordered_divide(%w(1 2 3 4 5 6 7 8 9),2).length
  end

  def test_collapse_ranges
    ranges = [(0..100), (50..150), (51..61),(200..250), (300..324),(320..350)]
    assert_equal [(0..150),(200..250), (300..350)], Misc.collapse_ranges(ranges)
  end

  def test_humanize
    str1 = "test_string"
    str2 = "TEST_string"
    str3 = "test"

    assert_equal "Test string", Misc.humanize(str1)
    assert_equal "TEST string", Misc.humanize(str2)
    assert_equal "Test", Misc.humanize(str3)

    assert_equal "mutation_enrichment", Misc.snake_case("MutationEnrichment")
  end

  def test_snake_case
    str1 = "ACRONIMTest"
    str2 = "ACRONIM_test"
    assert_equal "ACRONIM_test", Misc.snake_case(str1)
    assert_equal "ACRONIM_test", Misc.snake_case(str2)
  end

  def test_correct_vcf_mutations
    assert_equal [737407, ["-----", "-----G", "-----GTTAAT"]], Misc.correct_vcf_mutation(737406, "GTTAAT", "G,GG,GGTTAAT")
  end

  def test_fingerprint
    assert_equal '{:a=>1}', Misc.fingerprint({:a => 1})
  end

  def test_tarize
    path = File.expand_path('/home/mvazquezg/git/rbbt-util/lib')
    stream = Misc.tarize(path)
    TmpFile.with_file do |res|
      Misc.in_dir(res) do
        CMD.cmd("tar xvfz - ", :in => stream)
      end
    end
  end

  def test_camel_case
    assert_equal "DbSNP", Misc.camel_case("db_SNP")
    assert_equal "D3Js", Misc.camel_case("D3Js")
    assert_equal "Structure", Misc.camel_case("Structure")
    assert_equal "MutEval", Misc.camel_case("mut_eval")
    assert_equal "COSMIC", Misc.camel_case("COSMIC")
  end

  def test_texar
    ppp Misc.html_tag('textarea', "hola\nadios\nagain")
  end

  def __test_lock_fd
    require 'rbbt/workflow'
    Rbbt.var.jobs.Structure.neighbour_map.glob("*")[0..1000].each do |file|
      next if file =~ /\.info$/
      step = Step.new file
      TSV.open step.path
    end
    puts `ls -l /proc/#{ Process.pid }/fd`
  end

  def test_snake_case
    ppp Misc.snake_case("KinaseSARfari")
  end

  def test_bootstrap
    Log.severity = 0
    res = Misc.bootstrap((1..10).to_a, 2, :bar => "Test bootstrap ticks", :respawn => :always, :into => []) do |num|
      sleep 1 + rand(2)
      num
    end
  end

  def test_obj2md5
    str = "string" *  1000000
    obj1 = [1,2,"test", 0.3, [1,2,3], {:a => [1,2], :b => str}]
    obj2 = [1,2,"test", 0.3, [1,2,3], {:a => [1,2,3], :b => str}]
    obj3 = [1,2,:test, 0.3, [1,2,3], {:a => [1,2,"3"], :b => str}]
    assert_not_equal Misc.obj2md5(obj1), Misc.obj2md5(obj2)
    assert_equal Misc.obj2md5(obj2), Misc.obj2md5(obj3)
    hash = Hash[*(0..obj1.length).zip(obj1).flatten]

    Misc.benchmark(10000) do
      Misc.hash2md5(hash)
    end
    Misc.benchmark(10000) do
      Misc.obj2md5(obj1)
    end
    Misc.profile do
      10.times do
        Misc.hash2md5(hash)
      end
    end
    Misc.profile do
      10.times do
        Misc.obj2md5(obj1)
      end
    end
  end

  def test_obj2md5_str_float
    assert_equal Misc.obj2md5([1,2]), Misc.obj2md5(["1","2"])
  end

  def test_sample_large_string
    str = "string" *  1000000

    max = 100
    assert_equal Misc.sample_large_obj(str, max).length, max+51

    max = 1000
    assert_equal Misc.sample_large_obj(str, max).length, max+51
  end

  def test_sample_large_array
    str = (0..1000000).to_a

    max = 100
    assert_equal Misc.sample_large_obj(str, max).length, max+29

    max = 1000
    assert_equal Misc.sample_large_obj(str, max).length, max+29
  end

  def test_match_value
    assert Misc.match_value("10", "10")
    assert Misc.match_value("Hi", /hi/i)
    assert Misc.match_value("Hi", /Hi/)
    assert Misc.match_value("Hi", "/Hi/")
    assert Misc.match_value("Hi", "/hi/i")
    assert Misc.match_value("15", "<=20")
    assert Misc.match_value("15", ">14")
    assert Misc.match_value("15", "! >15")
    assert Misc.match_value("15", "! >15")
    assert Misc.match_value("15", [14, 15, 25])
    assert ! Misc.match_value("15", [14, 25])
  end
end
