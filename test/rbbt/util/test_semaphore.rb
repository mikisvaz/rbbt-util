require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

class TestRbbtSemaphore < Test::Unit::TestCase
  def test_process
    RbbtSemaphore.with_semaphore(10) do |s|
      pids = []
      100.times do 
        pids << Process.fork do
          100.times do
            RbbtSemaphore.wait_semaphore(s)
            sleep 0.001
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

