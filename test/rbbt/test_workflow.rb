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

  task :stream => :array do
    Misc.open_pipe do |sin|
      5.times do |i|
        sin.puts "line #{ i }"
        sleep 1
      end
    end
  end

  dep :stream
  task :stream2 => :array do
    TSV.get_stream step(:stream)
  end

  input :name, :string, "Name"
  task :input_dep => :text do |name|
    <<-EOF
Hi #{name}:
This is the input text
for this dependency
    EOF
  end

  input :text, :text, "Input text"
  task :reverse_input_text => :text do |text|
    text.reverse
  end

  dep :input_dep
  dep :reverse_input_text, :text => :input_dep
  task :send_input_dep_to_reverse => :text do
    TSV.get_stream step(:reverse_input_text)
  end
end

TestWF.workdir = Rbbt.tmp.test.workflow

class TestWorkflow < Test::Unit::TestCase

  def test_update_on_input_dependency_update
    send_input_dep_to_reverse_job = TestWF.job(:send_input_dep_to_reverse, nil, :name => "Miguel")
    send_input_dep_to_reverse_job.clean
    send_input_dep_to_reverse_job.run
    input_dep_job = send_input_dep_to_reverse_job.step(:input_dep)
    input_dep_job.clean
    input_dep_job.run
    send_input_dep_to_reverse_job = TestWF.job(:send_input_dep_to_reverse, nil, :name => "Miguel")
    mtime_orig = File.mtime send_input_dep_to_reverse_job.step(:reverse_input_text).path
    send_input_dep_to_reverse_job.run
    mtime_new = File.mtime send_input_dep_to_reverse_job.step(:reverse_input_text).path
    assert mtime_orig < mtime_new

  end

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

  def __test_stream
    io = TestWF.job(:stream).run(:stream)
    Misc.consume_stream(TSV.get_stream(io), false, STDOUT)
    nil
  end

  def __test_fork_stream
    job = TestWF.job(:stream)
    job.clean
    io = job.fork(:stream)
    Misc.consume_stream(TSV.get_stream(io), false, STDOUT)
    nil
  end

  def test_stream_order

    Log.with_severity 0 do
      job = TestWF.job(:stream2)
      job.recursive_clean
      job.produce

    end
  end


end
