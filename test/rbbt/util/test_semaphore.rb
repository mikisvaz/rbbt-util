require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/semaphore'

class TestRbbtSemaphore < Test::Unit::TestCase
  def test_thread
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

