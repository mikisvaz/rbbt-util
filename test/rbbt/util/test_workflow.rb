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

MathWF.basedir  = Rbbt.tmp.test.jobs.mathwf.find :user

module MathWF2
  extend WorkFlow
  task_option :value
  task :input => :integer do |value| value end

  task :add_1 => :integer do input + 1 end

  task :times_2 => :integer do input * 2 end

  task_dependencies :add_1
  task :times_4 => :integer do input * 4 end
  
  task_option :times, "Times to multiply by", :integer, 10
  task_dependencies :add_1
  task :times => :integer do |times| input * times end
end

MathWF2.basedir = Rbbt.tmp.test.jobs.mathwf.find :user

class TestWorkFlow < Test::Unit::TestCase
 
  def test_math_wf
    job = MathWF.tasks[:times_2].job(:job1, :value => 1)
    assert_equal 4, job.fork.join.load
    job = MathWF.tasks[:times_4].job(:job1, :value => 1)
    assert_equal 8, job.fork.join.load
  end

  def test_math_wf2
    job = MathWF2.tasks[:times_2].job(:job1, :value => 1)
    job.fork.join
    assert_equal 4, job.load
    job = MathWF2.tasks[:times_4].job(:job1, :value => 1)
    assert_equal 8, job.fork.join.load
  end

  def test_math_run
    job = MathWF2.job(:times_2, :job1,1)
    job.fork.join
    assert_equal 4, job.load
  end

  def test_math_defaults
    assert MathWF2.tasks[:times].option_defaults.include? :times

    job = MathWF2.job(:times, :job1, 1)
    job.fork.join
    assert_equal 20, job.load


    job = MathWF2.job(:times, :job1, 1, :times => 20)
    job.fork.join
    assert_equal 40, job.load
  end

end

