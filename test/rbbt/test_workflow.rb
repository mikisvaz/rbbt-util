require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/workflow'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestWF
  extend Workflow

  helper :user do
    "User"
  end 

  task :user => :string do
    user
  end

  str = "TEST"
  task :str => :string do
    str
  end

  dep :str
  task :reverse => :string do
    step(:str).load.reverse
  end

  dep :str
  task :downcase => :string do
    step(:str).load.downcase
  end

  dep :str
  input :times, :integer, "Times to repeat"
  task :repeat => :string do |times|
    [step(:str).load] * times * "\n"
  end

  input :number, :float, "Number to doble"
  def self.double(number)
    2 * number
  end
  task :double => :float

  desc <<-EOT
Returns numer * 2 lines containing TEST
  EOT
  dep :str
  dep :double
  task :repeat2 => :string do 
    [step(:str).load] * step(:double).load * "\n"
  end

  dep :str, :repeat, :repeat2
  task :double_dep => :array do
   [] << step(:str).load << step(:repeat).load << step(:repeat2).load
  end

  export_synchronous :double
  export_asynchronous :repeat2

  input :letter, :string, "Letter", "D"
  task :letter => :string do |l|
    l
  end

  dep :letter
  task :letter_repeat => :string do |l|
    ([step(:letter).load] * 2) * ""
  end

  dep :letter
  dep :letter_repeat, :letter => "A"
  task :two_letters => :string do
    dependencies.collect{|d| d.load } * ":"
  end



end

TestWF.workdir = Rbbt.tmp.test.workflow

class TestWorkflow < Test::Unit::TestCase

  def test_helper
    assert_equal "User", TestWF.job(:user, "Default", :number => 3).run
  end

  def test_job
    str = "TEST"
    job = TestWF.job(:repeat2, "Default", :number => 3).fork
    while not job.done?
      sleep 1
    end

    raise job.messages.last if job.error?

    assert_equal ["TEST"] * 6 * "\n", job.load
  end

  def test_with_subdir
    str = "TEST"
    job = TestWF.job(:repeat2, "Default", :number => 3).fork
    while not job.done?
      sleep 1
    end

    raise job.messages.last if job.error?

    assert_equal ["TEST"] * 6 * "\n", job.load
  end

  def test_search
    str = "TEST"
    job1 = TestWF.job(:repeat2, "subdir/Default", :number => 3).fork
    job2 = TestWF.job(:repeat2, "subdir/Other", :number => 3).fork
    job3 = TestWF.job(:repeat2, "Default", :number => 3).fork

    while not job1.done? and not job2.done? and not job3.done?
      sleep 1
    end

    assert_equal [job1.name, job2.name].sort, TestWF.jobs(:repeat2, "subdir/").sort
    assert_equal [job1.name].sort, TestWF.jobs(:repeat2, "subdir/Default")
    assert TestWF.jobs(:repeat2).include?(job1.name)
    assert TestWF.jobs(:repeat2).include?(job2.name)
    assert TestWF.jobs(:repeat2).include?(job3.name)
    assert TestWF.load_name(:repeat2, job3.name).done?
    assert_equal "TEST\nTEST\nTEST\nTEST\nTEST\nTEST", TestWF.load_name(:repeat2, TestWF.jobs(:repeat2).first).load
  end

  def test_double_dep
    assert_equal ["TEST", "TEST\nTEST", "TEST\nTEST\nTEST\nTEST"], TestWF.job(:double_dep, "foo", :times => 2, :number => 2).clean.run
  end

  def test_object_workflow
    a = ""
    a.extend Workflow
    a.task :foo => :string do
      "bar"
    end
    
    job = a.job(:foo)
    assert_equal 'bar', job.exec
  end

  def test_letter
    assert_equal "D", TestWF.job(:letter).run
    assert_equal "B", TestWF.job(:letter, nil, :letter => "B").run
    assert_equal "BB", TestWF.job(:letter_repeat, nil, :letter => "B").run
    job = TestWF.job(:two_letters, nil, :letter => "V")
    assert_equal "V:AA", job.run
  end

end
