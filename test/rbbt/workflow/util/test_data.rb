require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/util/data'

module TestDataWF2
  extend Workflow
  extend Workflow::Data

  input :salutation, :string
  task :salute_antonio => :string do |name,salutation|
    "Hi Antonio: #{salutation}"
  end
end

module TestDataWF
  extend Workflow
  extend Workflow::Data

  input :salutation, :string
  task :salute_luis => :string do |name,salutation|
    "Hi Luis: #{salutation}"
  end

  input :name, :string
  input :salutation, :string
  task :salute => :string do |name,salutation|
    "Hi #{name}: #{salutation}"
  end

  data_task :salute_data, TestDataWF, :salute, :salutation => :placeholder do |directory,options|
    options.merge({:salutation => directory.salutation.read})
  end

  data_task :salute_data2, TestDataWF, :salute, :salutation => :placeholder do |directory,options|
    {:task => :salute_luis, :inputs => options.merge({:salutation => directory.salutation.read})}
  end

  data_task :salute_data3, TestDataWF, :salute, :salutation => :placeholder do |directory,options|
    {:task => :salute_antonio, :workflow => TestDataWF2, :inputs => options.merge({:salutation => directory.salutation.read})}
  end

end

class TestWorkflowData < Test::Unit::TestCase
  def test_workflow_data
    TmpFile.with_file do |tmpdir|
      tmpdir = Path.setup(tmpdir.dup)

      Open.write(tmpdir.TestDir.options.name, "Miguel")
      Open.write(tmpdir.TestDir.salutation, "My salutations")

      TestDataWF.data_dir tmpdir

      job = TestDataWF.job(:salute_data, "TestDir")
      job.recursive_clean.run
      assert job.run.include? "Miguel"

      job = TestDataWF.job(:salute_data2, "TestDir")
      puts job.recursive_clean.run
      assert job.run.include? "Luis"

      job = TestDataWF.job(:salute_data3, "TestDir")
      job.recursive_clean.run
      assert job.run.include? "Antonio"
    end
  end
end

