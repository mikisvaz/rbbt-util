require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')

require 'rbbt-util'
require 'rbbt/util/log'
require 'rbbt/util/concurrency/processes'

class TestConcurrency < Test::Unit::TestCase

  def test_process
    q = RbbtProcessQueue.new 1

    res = []

    q.callback do |v|
      res << v
    end

    q.init do |i|
      i * 2
    end

    times = 500
    t = TSV.setup({"a" => 1}, :type => :single)

    Misc.benchmark do
    times.times do |i|
      q.process [i]
    end

    q.join
    q.clean
    end

    assert_equal times, res.length
    assert_equal [0, 2, 4], res.sort[0..2]
    
  end
end


