require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/concurrency'

class TestConcurrencyThreads < Test::Unit::TestCase
  def test_thread_queue
    q = RbbtThreadQueue.new 10

    res = []

    q.init do |v|
      res << v
    end

    times = 5_000_000
    Misc.benchmark do
    times.times do |i|
      q.process [i*2]
    end
    end

    q.join
    q.clean

    assert_equal times, res.length
    assert_equal [0, 2, 4], res.sort[0..2]
    
  end

  def test_each
    times = 50000
    elems = (0..times-1).to_a

    TmpFile.with_file do |dir|
      elems.each do |elem|
        Open.write(File.join(dir, elem.to_s), "DONE")
      end

      assert_equal times, Dir.glob(File.join(dir, '*')).length
    end
  end
end

