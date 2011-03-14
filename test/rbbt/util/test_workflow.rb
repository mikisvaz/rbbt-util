require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/workflow'

#module ReverseUppercaseWF
#  include WorkFlow
#
#  task_option :word, "Word to process", :string
#  task_persistence :string
#  task :task1 do |word|
#    word.reverse
#  end
#
#  task :task2 do 
#    input.uppercase
#  end
#end

module MathWF
  extend WorkFlow
  self.tasks[:input] = Task.new(:input, nil, :marshal, :value) do |value| value end
  self.tasks[:add_1] = Task.new(:add_1, nil, :marshal) do input + 1 end
  self.tasks[:add_1].dependencies << :input
  self.tasks[:times_2] = Task.new(:times_2, nil, :marshal) do input * 2 end
  self.tasks[:times_2].dependencies << :add_1
  self.tasks[:times_4] = Task.new(:times_4, nil, :marshal) do input * 4 end
  self.tasks[:times_4].dependencies << :add_1
  self.tasks.each do |name, task| task.workflow = self end
end

module MathWF2
  extend WorkFlow
  task_option :value
  task :input => :integer do |value| value end

  task :add_1 => :integer do input + 1 end

  task :times_2 => :integer do input * 2 end

  task_dependencies :add_1
  task :times_4 => :integer do input * 4 end
end

MathWF.basedir  = Rbbt.tmp.test.jobs.mathwf.find :user
MathWF2.basedir = Rbbt.tmp.test.jobs.mathwf.find :user

class TestWorkFlow < Test::Unit::TestCase
  
#  def _test_wf
#    assert ReverseUppercaseWF.tasks.include? :task1
#    #assert_equal "TSET", ReverseUppercaseWF.taks2 "job", "Test"
#  end


  def _test_math_wf
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



end

