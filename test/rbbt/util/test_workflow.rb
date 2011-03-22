require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/workflow'

module MathWF
  extend WorkFlow
  self.tasks[:input] = Task.new(:input, :marshal, :value) do |value| value end
  self.tasks[:add_1] = Task.new(:add_1, :marshal) do input + 1 end
  self.tasks[:add_1].dependencies << :input
  self.tasks[:times_2] = Task.new(:times_2, :marshal) do input * 2 end
  self.tasks[:times_2].dependencies << :add_1
  self.tasks[:times_4] = Task.new(:times_4, :marshal) do input * 4 end
  self.tasks[:times_4].dependencies << :add_1
  self.tasks.each do |name, task| task.workflow = self end
end


module MathWF2
  def mult(num, t)
    num * t
  end

  extend WorkFlow


  task_option :value
  task :input => :integer do |value| value end

  task :add_1 => :integer do input + 1 end

  task :times_2 => :integer do input * 2 end

  task_dependencies :add_1
  task :times_4 => :integer do input * 4 end
  
  task_option :times, "Times to multiply by", :integer, 10
  task_dependencies :add_1
  task :times => :integer do |times| mult(input, times) end

  task_dependencies []
  task :persist do  task.workflow.methods.include? "local_persist" end
end


module ConWF
  extend WorkFlow
  task :one => :integer do 1 end

  task_dependencies []
  task :two => :integer do 2 end

  task_option :init, "Initial value", :string
  task_dependencies  Proc.new{|jobname,run_options| 
    ConWF.job(run_options[:init].to_sym, jobname, {})
  }
  task :times_2 => :integer do 
    input * 2
  end
end

MathWF2.jobdir = Rbbt.tmp.test.jobs.mathwf.find :user
MathWF.jobdir  = Rbbt.tmp.test.jobs.mathwf.find :user
ConWF.jobdir   = Rbbt.tmp.test.jobs.mathwf.find :user

class TestWorkFlow < Test::Unit::TestCase
 
  def test_math_wf
    job = MathWF.tasks[:times_2].job(:job1, 1)
    assert_equal 4, job.fork.join.load
    job = MathWF.tasks[:times_4].job(:job1, 1)
    assert_equal 8, job.fork.join.load
  end

  def test_math_wf2
    job = MathWF2.tasks[:times_2].job(:job1, 1)
    job.fork.join
    assert_equal 4, job.load
    job = MathWF2.tasks[:times_4].job(:job1, 1)
    assert_equal 8, job.fork.join.load
  end

  def test_math_run
    job = MathWF2.job(:times_2, :job1, 1)
    job.fork.join
    assert_equal 4, job.load
  end

  def test_math_defaults
    assert MathWF2.tasks[:times].option_defaults.include? :times

    job = MathWF2.job(:times, :job1, 1)
    job.fork.join
    assert job.done?
    puts job.messages
    assert File.exists? job.path
    assert_equal 20, job.load


    job = MathWF2.job(:times, :job1, 1, :times => 20)
    job.fork.join
    assert_equal 40, job.load
  end

  def test_recursive_clean
    job = MathWF2.job(:times, :job1, 1, :times => 20)
    job.fork.join
    assert File.exists?(job.path)
    job.clean
    assert (not File.exists?(job.path))
    assert File.exists?(job.previous_jobs.first.path)
    job.recursive_clean
    assert (not File.exists?(job.previous_jobs.first.path))
  end

  def test_local_persist
    assert MathWF2.run(:persist, :persist).load
  end

  def test_conditional
    assert 2, ConWF.run(:times_2, "Test", "one").load
    assert 4, ConWF.run(:times_2, "Test", "two").load
  end
end

