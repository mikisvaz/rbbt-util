require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class TestTSVParallelThrough < Test::Unit::TestCase

  def test_traverse_tsv
    require 'rbbt/sources/organism'

    head = 100

    tsv = Organism.identifiers("Hsa").tsv :head => head
    res = {}
    TSV.traverse tsv do |k,v|
       res[k] = v
    end
    assert_equal head, res.keys.compact.sort.length

    tsv = Organism.identifiers("Hsa").tsv :head => head
    TSV.traverse tsv, :into => res do |k,v|
      [k,v]
    end
    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_tsv_cpus
    require 'rbbt/sources/organism'

    head = 100

    tsv = Organism.identifiers("Hsa").tsv :head => head
    res = {}
    TSV.traverse tsv do |k,v|
       res[k] = v
    end
    assert_equal head, res.keys.compact.sort.length
    assert res.values.compact.flatten.uniq.length > 0

    tsv = Organism.identifiers("Hsa").tsv :head => head
    TSV.traverse tsv, :into => res, :cpus => 5 do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
    assert res.values.compact.flatten.uniq.length > 0
  end

  def test_traverse_stream
    require 'rbbt/sources/organism'

    head = 1000

    tsv = Organism.identifiers("Hsa").open
    res = {}
    TSV.traverse tsv, :head => head, :into => res do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_stream_cpus
    require 'rbbt/sources/organism'

    head = 1000

    tsv = Organism.identifiers("Hsa")
    res = {}
    TSV.traverse tsv, :head => head, :cpus => 5, :into => res do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
  end

  def test_traverse_stream_keys
    require 'rbbt/sources/organism'

    head = 1000

    tsv = Organism.identifiers("Hsa").open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys do |v|
       res << v
    end

    assert_equal res, Organism.identifiers("Hsa").tsv(:head => head).keys

    tsv = Organism.identifiers("Hsa").open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys, :into => res do |v|
      v
    end

    assert_equal res.sort, Organism.identifiers("Hsa").tsv(:head => head).keys.sort
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

    head = 2_000

    tsv = Organism.identifiers("Hsa").open
    Misc.benchmark do
      res = {}
      TSV.traverse tsv, :head => head do |k,v|
        res[k] = v
      end
    end

    tsv = Organism.identifiers("Hsa").open
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

    stream = Organism.identifiers("Hsa").open 
    dumper = TSV::Dumper.new Organism.identifiers("Hsa").tsv_options
    dumper.init
    TSV.traverse stream, :head => head, :into => dumper do |k,v|
      k = k.first
      [k,v]
    end

    res = TSV.open(dumper.stream)

    assert_equal head, res.size
  end

  def test_traverse_into_dumper_threads
    require 'rbbt/sources/organism'

    head = 2_000
    threads = 10

    stream = Organism.identifiers("Hsa").open 
    dumper = TSV::Dumper.new Organism.identifiers("Hsa").tsv_options
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

    stream = Organism.identifiers("Hsa").open 
    dumper = TSV::Dumper.new Organism.identifiers("Hsa").tsv_options
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

    tsv = TSV::Parser.new Organism.identifiers("Hsa").open, :head => head
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

    tsv = TSV::Parser.new Organism.identifiers("Hsa").open, :head => head
    dumper = TSV::Dumper.new tsv.options

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

    head = 10_000
    cpus = nil

    stream = Organism.identifiers("Hsa").open 
    dumper = TSV::Dumper.new Organism.identifiers("Hsa").tsv_options

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

  def test_traverse_dumper_cpus_exception
    require 'rbbt/sources/organism'

    head = 2_000
    cpus = 2

    stream = Organism.identifiers("Hsa/jun2011").open 
    dumper = TSV::Dumper.new Organism.identifiers("Hsa/jun2011").tsv_options

    assert_raise do
      begin
      TSV.traverse stream, :head => head,  :cpus => cpus, :into => dumper do |k,v|
        k = k.first
        raise "STOP" if rand(100) < 1
        [k,v]
      end
      dumper.stream.read
      rescue Exception
        Log.exception $!
        raise $!
      end
    end
  end
end
