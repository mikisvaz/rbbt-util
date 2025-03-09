require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

require 'scout/workflow'

class TestWRecursive < Test::Unit::TestCase
  def _test_rec_inputs
    m = Module.new do
      extend Workflow
      self.name = "TestWF"

      input :option1
      task :step1 do end

      dep :step1
      input :option2
      task :step2 do end
    end

    assert_include m.rec_inputs(:step2), :option2
  end

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
    info = m.task_info :step2
    ppp info.to_json

  end
end

