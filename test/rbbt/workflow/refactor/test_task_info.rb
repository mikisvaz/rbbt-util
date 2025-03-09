require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require 'scout/workflow'
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')


class TestTaskInfo < Test::Unit::TestCase
  def test_task_info
    m = Module.new do
      extend Workflow
      self.name = "TestWF"

      input :option1
      task :step1 do end

      dep :step1
      input :option2
      task :step2 do end
    end

    iii m.task_info :step1

  end
end

