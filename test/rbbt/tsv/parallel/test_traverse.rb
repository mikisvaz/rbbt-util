require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/tsv'
require 'rbbt/tsv/parallel'

class TestTSVParallelThrough < Test::Unit::TestCase

  def _test_traverse_tsv
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

  def _test_traverse_stream
    require 'rbbt/sources/organism'

    head = 100

    tsv = Organism.identifiers("Hsa").open
    res = {}
    TSV.traverse tsv, :head => head do |k,v|
       res[k] = v
    end

    assert_equal head, res.keys.compact.sort.length
    tsv = Organism.identifiers("Hsa").open
    res = {}
    TSV.traverse tsv, :head => head, :into => res do |k,v|
      [k,v]
    end

    assert_equal head, res.keys.compact.sort.length
  end

  def _test_traverse_stream_keys
    require 'rbbt/sources/organism'

    head = 100

    tsv = Organism.identifiers("Hsa").open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys do |v|
       res << v
    end

    assert_equal res, Organism.identifiers("Hsa").tsv(:head => 100).keys

    tsv = Organism.identifiers("Hsa").open
    res = []

    TSV.traverse tsv, :head => head, :type => :keys, :into => res do |v|
      v
    end

    assert_equal res, Organism.identifiers("Hsa").tsv(:head => 100).keys
  end
  
  def _test_traverse_array
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

  def _test_traverse_array_threads
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

  def _test_traverse_array_cpus
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

    head = 80_000

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
end
