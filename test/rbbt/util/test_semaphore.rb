require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/semaphore'

class TestRbbtSemaphore < Test::Unit::TestCase
  def test_process
    RbbtSemaphore.with_semaphore(10) do |s|
      pids = []
      100.times do 
        pids << Process.fork do
          100.times do
            RbbtSemaphore.wait_semaphore(s)
            sleep 0.001
            puts '.'
            RbbtSemaphore.post_semaphore(s)
          end
        end
      end
      pids.each do |pid|
        Process.waitpid pid
      end
    end
  end

  def _test_thread
    times = 50
    TmpFile.with_file do |dir|
      Path.setup(dir)
      FileUtils.mkdir_p dir
      RbbtSemaphore.thread_each_on_semaphore((1..times).to_a, 25){|elem|
        sleep rand
        Open.write(dir[elem], "test")
      }
      assert_equal times, dir.glob.length
    end
  end
end

