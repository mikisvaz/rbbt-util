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
end

