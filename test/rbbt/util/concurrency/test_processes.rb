require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')

require 'rbbt-util'
require 'rbbt/util/log'
require 'rbbt/util/concurrency/processes'

class TestConcurrencyProcess < Test::Unit::TestCase

  def test_process
    q = RbbtProcessQueue.new 10

    res = []

    q.callback do |v|
      res << v
    end

    q.init do |i|
      i * 2
    end

    times = 500
    t = TSV.setup({"a" => 1}, :type => :single)

    times.times do |i|
      q.process i
    end

    q.join

    assert_equal times, res.length
    assert_equal [0, 2, 4], res.sort[0..2]
  end

  def test_each
    times = 5000
    elems = (0..times-1).to_a

    TmpFile.with_file do |dir|
      RbbtProcessQueue.each(elems) do |elem|
        Open.write(File.join(dir, elem.to_s), "DONE")
      end

      assert_equal times, Dir.glob(File.join(dir, '*')).length
    end
  end

  def test_process_abort
    assert_raise Aborted do
      q = RbbtProcessQueue.new 10

      res = []

      q.callback do |v|
        res << v
      end

      q.init do |i|
        sleep 1 while true
      end

      times = 500
      t = TSV.setup({"a" => 1}, :type => :single)

      times.times do |i|
        q.process i
      end

      sleep 2
      q.clean


      q.join
    end
  end


  def test_process_respawn
    q = RbbtProcessQueue.new 2, nil, nil, true

    res = []

    q.callback do |v|
      res << v
    end

    q.init do |i|
      $str ||="-"
      $str = $str *  (2 + rand(5).to_i)
      sleep 0.1
      "."
    end

    times = 50

    times.times do |i|
      q.process i
    end

    q.join
  end
end


