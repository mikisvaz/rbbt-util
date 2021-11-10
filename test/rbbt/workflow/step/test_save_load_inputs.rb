require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/step/save_load_inputs'

ENV["RBBT_DEBUG_JOB_HASH"] = true.to_s
require 'rbbt/workflow'
module TestSaveLoadWF
  extend Workflow

  task :list => :array do
    (0..10).to_a.collect{|e| e.to_s}
  end

  input :elist, :array
  task :reverse => :array do |list|
    list.reverse
  end

  dep :list
  dep :reverse, :elist => :list
  task :prefix => :array do
    step(:reverse).run.collect{|e| "A-#{e}" }
  end
end

class TestSaveLoad < Test::Unit::TestCase
  def test_save
    Log.with_severity 0 do
    job = TestSaveLoadWF.job(:prefix)
    job.produce
    TmpFile.with_file do |directory|
      Step.save_job_inputs(job.step(:reverse), directory)
      newjob = TestSaveLoadWF.job_for_directory_inputs(:reverse, directory)
      assert_equal job.rec_dependencies.last.path, newjob.path
    end
    end
  end
end

