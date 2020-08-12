require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class StopException < StandardError; end

class TestTSVParallelThrough < Test::Unit::TestCase

  def test_traverse_tsv
    require 'rbbt/sources/organism'

    head = 100

    tsv = datafile_test('identifiers').tsv :head => head
    res = {}
    TSV.traverse tsv do |k,v|
       res[k] = v
    end
    assert_equal head, res.keys.compact.sort.length

    tsv = datafile_test('identifiers').tsv :head => head
    TSV.traverse tsv, :into => res do |k,v|
      [k,v]
    end
    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_tsv_cpus
    require 'rbbt/sources/organism'

    head = 100

    tsv = datafile_test('identifiers').tsv :head => head
    res = {}
    TSV.traverse tsv do |k,v|
       res[k] = v
    end
    assert_equal head, res.keys.compact.sort.length
    assert res.values.compact.flatten.uniq.length > 0

    tsv = datafile_test('identifiers').tsv :head => head
    TSV.traverse tsv, :into => res, :cpus => 5 do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
    assert res.values.compact.flatten.uniq.length > 0
  end

  def test_traverse_stream
    require 'rbbt/sources/organism'

    head = 1000

    tsv = datafile_test('identifiers').open
    res = {}
    TSV.traverse tsv, :head => head, :into => res do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_stream_cpus
    require 'rbbt/sources/organism'

    head = 1000

    tsv = datafile_test('identifiers')
    res = {}
    TSV.traverse tsv, :head => head, :cpus => 5, :into => res do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_stream_keys
    require 'rbbt/sources/organism'

    head = 1000

    tsv = datafile_test('identifiers').open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys do |v|
       res << v
    end

    assert_equal res, datafile_test('identifiers').tsv(:head => head).keys

    tsv = datafile_test('identifiers').open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys, :into => res do |v|
      v
    end

    assert_equal res.sort, datafile_test('identifiers').tsv(:head => head).keys.sort
  end
  
  def test_traverse_array
    require 'rbbt/sources/organism'

    array = []
    100.times do array << rand(10).to_i end

    res = []
    TSV.traverse array do |v|
       res << v
    end

    assert_equal array, res


    res = []
    TSV.traverse array, :into => res do |v|
      v
    end

    assert_equal array, res
  end
  
  def test_traverse_priority
    require 'fc'

    queue = FastContainers::PriorityQueue.new(:min)

    array = []
    100.times do e = rand(1000).to_i; array << e; queue.push(e,e) end

    res = TSV.traverse queue, :into => [] do |v|
      v
    end

    assert_equal array.sort, res
  end

  def test_traverse_array_threads
    require 'rbbt/sources/organism'

    array = []
    100.times do array << rand(10).to_i end

    res = []
    TSV.traverse array, :threads => 5 do |v|
       res << v
    end

    assert_equal array.sort, res.sort

    res = []
    TSV.traverse array, :threads => 5, :into => res do |v|
      v
    end

    assert_equal array.sort, res.sort
  end

  def test_traverse_array_cpus
    require 'rbbt/sources/organism'

    array = []
    100.times do array << rand(10).to_i end

    res = []

    TSV.traverse array, :cpus => 5, :into => res do |v|
      v
    end

    assert_equal array.sort, res.sort
  end

  def test_traverse_benchmark
    require 'rbbt/sources/organism'

    head = 5_000

    tsv = datafile_test('identifiers').open
    Misc.benchmark do
      res = {}
      TSV.traverse tsv, :head => head do |k,v|
        res[k] = v
      end
    end

    tsv = datafile_test('identifiers').open
    Misc.benchmark do
      res = {}
      TSV.traverse tsv, :head => head, :cpus => 5, :into => res do |k,v|
        [k,v]
      end
    end
  end

  def test_traverse_into_dumper
    require 'rbbt/sources/organism'

    head = 2_000

    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init
    TSV.traverse stream, :head => head, :into => dumper, :bar => true do |k,v|
      k = k.first
      [k,v]
    end

    res = TSV.open(dumper.stream)

    assert_equal head, res.size
  end

  def test_traverse_into_dumper_error_bar
    require 'rbbt/sources/organism'

    head = 2_000

    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init
    TSV.traverse stream, :head => head, :into => dumper, :bar => true do |k,v|
      k = k.first
      raise 
      [k,v]
    end

    assert_raise do 
      res = TSV.open(dumper.stream)
    end

  end

  def test_traverse_into_dumper_threads
    require 'rbbt/sources/organism'

    head = 2_000
    threads = 10

    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init

    TSV.traverse stream, :threads => threads, :head => head, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end

    res = TSV.open(StringIO.new(dumper.stream.read))

    assert_equal head, res.size
  end

  def test_traverse_into_dumper_cpus
    require 'rbbt/sources/organism'

    head = 2_000
    cpus = 10

    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init
    TSV.traverse stream, :cpus => cpus, :head => head, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end

    res = TSV.open(dumper.stream)

    assert_equal head, res.size
  end

  #{{{ TRAVERSE DUMPER

  def test_traverse_dumper
    require 'rbbt/sources/organism'

    head = 2_000

    tsv = TSV::Parser.new datafile_test('identifiers').open, :head => head
    dumper = TSV::Dumper.new tsv.options

    TSV.traverse tsv, :head => head, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end
    
    res = {}
    TSV.traverse dumper.stream, :into => res do |k,v|
      [k, v.length]
    end

    assert_equal head, res.size
  end

  def test_traverse_dumper_threads
    require 'rbbt/sources/organism'

    head = 2_000
    threads = 3

    tsv = TSV::Parser.new datafile_test('identifiers').open, :head => head

    dumper = TSV::Dumper.new tsv.options
    dumper.init

    TSV.traverse tsv, :head => head, :threads => threads, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end
    
    res = {}
    TSV.traverse dumper.stream, :threads => threads, :into => res do |k,v|
      [k, v.length]
    end

    assert_equal head, res.size
  end

  def test_traverse_dumper_cpus
    require 'rbbt/sources/organism'

    head = 5_000
    cpus = 4

    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init

    TSV.traverse stream, :head => head, :cpus => cpus, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end

    res = {}
    TSV.traverse dumper.stream, :cpus => cpus, :into => res do |k,v|
      [k, v.length]
    end

    
    assert_equal head, res.size
  end

  def test_traverse_dumper_exception
    require 'rbbt/sources/organism'

    head = 2_000

    Log.info Log.color :red, "TRAVERSE EXCEPTION"
    stream = datafile_test('identifiers').open
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init

    assert_raise StopException do
      TSV.traverse stream, :head => head, :into => dumper do |k,v|
        k = k.first
        raise StopException if rand(100) < 20
        [k,v]
      end
      dumper.stream.join
    end
  end

  def test_traverse_dumper_cpus_exception
    require 'rbbt/sources/organism'

    head = 2_000
    cpus = 2

    Log.info Log.color :red, "TRAVERSE EXCEPTION"
    stream = datafile_test('identifiers').open 
    dumper = TSV::Dumper.new datafile_test('identifiers').tsv_options
    dumper.init

    assert_raise ProcessFailed do
      TSV.traverse stream, :head => head, :cpus => cpus, :into => dumper do |k,v|
        k = k.first
        raise ProcessFailed if rand(100) < 20
        [k,v]
      end
      dumper.stream.join
    end
  end

  def test_traverse_into_stream
    size = 100
    array = (1..size).to_a.collect{|n| n.to_s}
    stream = TSV.traverse array, :into => :stream do |e|
      e
    end
    assert_equal size, stream.read.split("\n").length
  end

  def test_traverse_progress
    size = 1000
    array = (1..size).to_a.collect{|n| n.to_s}
    stream = TSV.traverse array, :bar => {:max => size, :desc => "Array"}, :cpus => 5, :into => :stream do |e|
      sleep 0.001
      e
    end
    assert_equal size, stream.read.split("\n").length

    size = 1000
    array = (1..size).to_a.collect{|n| n.to_s}
    stream = TSV.traverse array, :bar => {:max => size, :desc => "Array"}, :cpus => 5, :into => :stream do |e|
      sleep 0.001
      e
    end
    assert_equal size, stream.read.split("\n").length

    size = 1000
    array = (1..size).to_a.collect{|n| n.to_s}
    stream = TSV.traverse array, :bar => {:max => size, :desc => "Array"}, :cpus => 5, :into => :stream do |e|
      sleep 0.001
      e
    end
    assert_equal size, stream.read.split("\n").length

    size = 1000
    array = (1..size).to_a.collect{|n| n.to_s}
    stream = TSV.traverse array, :bar => {:max => size, :desc => "Array"}, :cpus => 5, :into => :stream do |e|
      sleep 0.01
      e
    end
    assert_equal size, stream.read.split("\n").length
  end

  def test_store_multiple
    size = 1000
    array = (1..size).to_a.collect{|n| n.to_s}

    stream = TSV.traverse array, :bar => {:max => size, :desc => "Multiple into stream"}, :cpus => 2, :into => :stream do |e|
      sleep 0.001
      res = [e,e+".alt"]
      res.extend MultipleResult
      res
    end

    assert_equal size*2, stream.read.split("\n").length
  end
end
