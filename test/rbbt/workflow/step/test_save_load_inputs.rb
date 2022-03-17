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

  input :integer, :integer
  input :float, :float
  input :file, :file
  input :file_no_file, :file, "", nil, :nofile => true
  input :string, :string
  input :select, :select
  input :array, :array
  input :array_no_file, :array, "", nil, :nofile => true
  input :tsv, :tsv, "", nil, :nofile => true
  input :tsv_no_file, :tsv, "", nil, :nofile => true
  input :binary, :binary, "", nil, :nofile => true
  input :binary_no_file, :tsv, "", nil, :nofile => true
  task :save_test => :string do
    "DONE"
  end
end

class TestSaveLoad < Test::Unit::TestCase
  def test_save
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

  def test_save_all_normal
    tsv = TSV.setup({}, "Key~Value#:type=:single")
    tsv["key"] = "value"
    options = {
      :float => 0.5,
      :integer => 15,
      :string => "STRING",
      :select => "option",
      :array => [0,1,2,3],
      :tsv => tsv,
    }
    Log.with_severity 0 do
      Misc.with_env "RBBT_DEBUG_JOB_HASH", "true" do
        job = TestSaveLoadWF.job(:save_test, nil, options)
        TmpFile.with_file do |directory|
          Step.save_job_inputs(job, directory)
          newjob = TestSaveLoadWF.job_for_directory_inputs(:save_test, directory)
          assert_equal job.path, newjob.path
        end
      end
    end
  end

  def test_save_all_file
    tsv = TSV.setup({}, "Key~Value#:type=:single")
    tsv["key"] = "value"
    Log.with_severity 0 do
      Misc.with_env "RBBT_DEBUG_JOB_HASH", "true" do
        TmpFile.with_file do |input_file|
          Path.setup(input_file)
          options = {
            :float => input_file.float,
            :integer => input_file.integer,
            :string => input_file.string,
            :select => input_file.select,
            :array => input_file.array,
            :tsv => input_file.tsv_file,
          }
          job = TestSaveLoadWF.job(:save_test, nil, options)
          TmpFile.with_file do |directory|
            Step.save_job_inputs(job, directory)
            newjob = TestSaveLoadWF.job_for_directory_inputs(:save_test, directory)
            assert_equal job.path, newjob.path
          end
        end
      end
    end
  end

  def test_save_all_job_file
    tsv = TSV.setup({}, "Key~Value#:type=:single")
    tsv["key"] = "value"
    Log.with_severity 0 do
      Misc.with_env "RBBT_DEBUG_JOB_HASH", "true" do
        TmpFile.with_file do |input_file|
          Path.setup(input_file)
          options = {
            :float => input_file.float,
            :integer => input_file.integer,
            :string => input_file.string,
            :select => input_file.select,
            :array => TestSaveLoadWF.job(:list),
            :binary => TestSaveLoadWF.job(:list).file('binary'),
            :tsv => input_file.tsv_file,
          }
          job = TestSaveLoadWF.job(:save_test, nil, options)
          TmpFile.with_file do |directory|
            Step.save_job_inputs(job, directory)
            newjob = TestSaveLoadWF.job_for_directory_inputs(:save_test, directory)
            assert_equal job.path, newjob.path
          end
        end
      end
    end
  end
end

