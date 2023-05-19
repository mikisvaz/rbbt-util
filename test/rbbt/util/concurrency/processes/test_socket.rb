require File.join(File.expand_path(File.dirname(__FILE__)), '../../../..', 'test_helper.rb')

require 'rbbt-util'
require 'rbbt/util/log'
require 'rbbt/util/concurrency/processes/socket'

class TestConcurrency < Test::Unit::TestCase
  def test_socket_push_pop
    obj1 = [1,[2,3,4]] #* 1000
    obj2 = ["1",["2","3","4"]] #* 1000
    obj3 = "some string"
    obj4 = TSV.setup({1 => 1})

    socket = RbbtProcessQueue::RbbtProcessSocket.new  Marshal
    10.times do
      socket.push(obj1)
      socket.push(obj2)
      socket.push(obj3)
      socket.push(obj4)

      assert_equal obj1, socket.pop
      assert_equal obj2, socket.pop 
      assert_equal obj3, socket.pop 
      assert_equal obj4, socket.pop 

    end

    socket.swrite.close
    assert_raise ClosedStream do 
      socket.pop
    end

    socket.clean
  end
end

if false and __FILE__ == $0
  socket = RbbtProcessQueue::RbbtProcessSocket.new 

  obj = "Some string" * 1000
  Misc.benchmark(1000) do
    socket.push(obj)
    socket.pop
  end

  obj = ["Some string"] * 1000
  Misc.benchmark(1000) do
    socket.push(obj)
    socket.pop
  end
  socket.clean


  socket = RbbtProcessQueue::RbbtProcessSocket.new Marshal

  obj = "Some string" * 1000
  Misc.benchmark(1000) do
    socket.push(obj)
    socket.pop
  end
  socket.clean

  socket = RbbtProcessQueue::RbbtProcessSocket.new TSV::StringArraySerializer
  obj = ["Some string"] * 1000
  Misc.benchmark(1000) do
    socket.push(obj)
    socket.pop
  end
  socket.clean
end
