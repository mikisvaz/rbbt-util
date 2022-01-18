require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/step/save_load_inputs'

ENV["RBBT_DEBUG_JOB_HASH"] = true.to_s
require 'rbbt/workflow'
module TestSaveLoadWF
  extend Workflow

  task :number => :integer do
    10
  end

  task :list => :array do
    (0..10).to_a.collect{|e| e.to_s}
  end

  input :list, :array
  input :number, :integer
  task :reverse => :array do |list|
    list.reverse
  end

  dep :list
  dep :number
  dep :reverse, :list => :list, :number => :number
  task :prefix => :array do
    step(:reverse).run.collect{|e| "A-#{e}" }
  end
end

class TestSaveLoad < Test::Unit::TestCase
  def test_save
    Log.with_severity 0 do
    job = TestSaveLoadWF.job(:prefix)
    job.recursive_clean
    job = TestSaveLoadWF.job(:prefix)
    TmpFile.with_file do |directory|
      Step.save_job_inputs(job.step(:reverse), directory)
      job.produce
      newjob = TestSaveLoadWF.job_for_directory_inputs(:reverse, directory)
      assert_equal job.rec_dependencies.last.path, newjob.path
    end
    end
  end
end

