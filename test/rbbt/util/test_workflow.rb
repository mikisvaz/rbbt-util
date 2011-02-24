require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt'
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'
require 'rbbt/util/workflow'

class TestWorkflow < Test::Unit::TestCase

  def workflow
    TmpFile.with_file do |dir|
      old_pdw = FileUtils.pwd
      begin
        FileUtils.mkdir dir
        cd dir
        yield dir
      ensure
        cd old_pdw
      end
    end
  end

  def test_true
    assert true
  end

  def _test_run
    workflow do |dir|
      WorkFlow.run do
        file :foo do |t|
          touch t.name
        end
      end
      assert File.exists? File.join(dir, 'foo')
    end
  end

  def _test_step
    jobid = 'jobid'
    target_step = 'last'
    workflow do |dir|

      WorkFlow.run(File.join(target_step, jobid)) do
        self.extend WorkFlow::Runner

        step :first do 
          "Test"
        end

        step :last do 
          data.reverse
        end
      end
      assert File.exists? File.join(dir, target_step, 'jobid')
      assert_equal 'tseT', Open.read(File.join(dir, target_step, 'jobid'))
    end
  end

  def _test_input
    jobid = 'jobid'
    target_step = 'last'
    message = "Message"

    workflow do |dir|

      WorkFlow.run(File.join(target_step, jobid), message) do
        self.extend WorkFlow::Runner

        step :first, :marshal do 
          input
        end

        step :last do 
          data.reverse
        end
      end

      assert File.exists? File.join(dir, target_step, 'jobid')
      assert_equal message.reverse, Open.read(File.join(dir, target_step, 'jobid'))
    end
  end
end

