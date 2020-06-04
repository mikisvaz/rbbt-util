require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow'

module DepWorkflow
  extend Workflow

  input :input_file, :file, "Input file", nil, :stream => true
  task :s1 => :array do |input_file|
    TSV.traverse input_file, :type => :array, :into => :stream, :bar => "Task1" do |line|
      line + "\t" << "Task1"
    end
  end

  dep :s1
  task :s2 => :array do |input_file|
    TSV.traverse step(:s1), :type => :array, :into => :stream, :bar => "Task2" do |line|
      next [line.split("\t").first, Misc::SKIP_TAG] * "\t" if rand < 0.9
      line + "\t" << "Task2"
    end
  end

  dep :s1
  dep :s2
  task :s3 => :array do |input_file|
    Misc.paste_streams(dependencies.reverse)
  end

  input :input_file, :file, "Input file", nil, :stream => true
  task :task1 => :array do |input_file|
    TSV.traverse input_file, :type => :array, :into => :stream, :bar => "Task1" do |line|
      line + "\t" << "Task1"
    end
  end

  dep :task1
  task :task2 => :array do
    TSV.traverse step(:task1), :type => :array, :into => :stream, :bar => "Task2" do |line|
      line + "\t" << "Task2"
    end
  end

  dep :task1
  task :task3 => :array do
    TSV.traverse step(:task1), :type => :array, :into => :stream, :bar => "Task3" do |line|
      line + "\t" << "Task3"
    end
  end

  dep :task2
  dep :task3
  task :task4 => :array do
    Misc.paste_streams(dependencies)
  end

  dep :task4
  task :task5 => :array do
    TSV.traverse step(:task4), :type => :array, :into => :stream, :bar => "Task5" do |line|
      line + "\t" << "Task5"
    end
  end

  dep :task2
  dep :task5
  task :task6 => :array do
    Misc.paste_streams(dependencies)
  end

  input :stream_file, :file, "Streamed file", nil, :stream => true
  task :task7 => :array do |file|
    TSV.traverse file, :type => :array, :into => :stream, :bar => "Task7" do |line|
      line + "\t" << "Task7"
    end
  end

  dep :task6
  dep :task7, :stream_file => :task6
  task :task8 => :array do
    TSV.get_stream step(:task7)
  end
end

module ComputeWorkflow
  extend Workflow

  input :input_file, :file, "Input file", nil, :stream => true
  task :task1 => :array do |input_file|
    TSV.traverse input_file, :type => :array, :into => :stream, :bar => "Task1" do |line|
      line + "\t" << "Task1"
    end
  end

  dep :task1, :compute => :produce
  task :task2 => :array do
    TSV.traverse step(:task1), :type => :array, :into => :stream, :bar => "Task2" do |line|
      line + "\t" << "Task2"
    end
  end

end

module ResumeWorkflow
  extend Workflow

  resumable
  task :resume => :string do
    if file('foo').exists?
      'done'
    else
      Open.mkdir files_dir
      Open.touch(file('foo'))
      raise 
    end
  end

  dep :resume
  task :reverse => :string do
    step(:resume).load.reverse
  end
end

class TestWorkflowDependency < Test::Unit::TestCase
  def test_task1
    size = 10000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    TmpFile.with_file(content) do |input_file|
      job = DepWorkflow.job(:task1, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream)
      last_line = nil
      while line = io.gets
        last_line = line.strip
      end
      io.join

      assert_equal "Line #{size}\tTask1", last_line
    end
  end

  def test_task2
    size = 10000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    TmpFile.with_file(content) do |input_file|
      job = DepWorkflow.job(:task2, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream)
      last_line = nil
      while line = io.gets
        last_line = line.strip
      end
      io.join

      assert_equal "Line #{size}\tTask1\tTask2", last_line
    end
  end

  def test_task3
    size = 100000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    TmpFile.with_file(content) do |input_file|
      job = DepWorkflow.job(:task3, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream)
      last_line = nil
      while line = io.gets
        last_line = line.strip
      end
      io.join

      assert_equal "Line #{size}\tTask1\tTask3", last_line
    end
  end

  def test_task4
    size = 100000
    Log.severity = 0
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    last_line = nil
    TmpFile.with_file(content) do |input_file|
      job = DepWorkflow.job(:task4, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream) 
      while line = io.gets 
        last_line = line.strip
      end
      io.join
    end

    assert_equal "Line #{size}\tTask1\tTask2\tTask1\tTask3", last_line
  end
  
  def test_task5
    size = 10000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    last_line = nil
    TmpFile.with_file(content) do |input_file|
      job = DepWorkflow.job(:task5, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream)
      while line = io.gets
        last_line = line.strip
      end
      io.join
    end
    assert_equal "Line #{size}\tTask1\tTask2\tTask1\tTask3\tTask5", last_line
  end
  
  def test_s3
    size = 100000
    content = (1..size).to_a.collect{|num| "Line #{num}" } * "\n"
    last_line = nil
    Log.severity = 0
    TmpFile.with_file(content) do |input_file|
      begin
        job = DepWorkflow.job(:s3, "TEST", :input_file => input_file)
        job.recursive_clean
        job.run(:stream)
        io = TSV.get_stream job
        while line = io.gets
          last_line = line.strip
        end
        io.join if io.respond_to? :join
      rescue Exception
        job.abort
        raise $!
      end
    end
    assert last_line.include? "Line #{size}"
  end
  
  def test_task6
    size = 100000
    content = (1..size).to_a.collect{|num| "Line #{num}" } * "\n"
    last_line = nil
    Log.severity = 0
    TmpFile.with_file(content) do |input_file|
      begin
        job = DepWorkflow.job(:task6, "TEST", :input_file => input_file)
        job.recursive_clean
        job.run(:stream)
        io = TSV.get_stream job
        while line = io.gets
          last_line = line.strip
        end
        io.join
      rescue Exception
        job.abort
        raise $!
      end
    end
    assert_equal "Line #{size}\tTask1\tTask2\tTask1\tTask2\tTask1\tTask3\tTask5", last_line
  end
  
  def test_task8
    size = 10000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    last_line = nil
    Log.severity = 0
    TmpFile.with_file(content) do |input_file|
      begin
      job = DepWorkflow.job(:task8, "TEST", :input_file => input_file)
      job.run(:stream)
      io = TSV.get_stream job
      while line = io.gets
        last_line = line.strip
      end
      io.join
      rescue Exception
        job.abort
        raise $!
      end
    end
    assert_equal "Line #{size}\tTask1\tTask2\tTask1\tTask2\tTask1\tTask3\tTask5\tTask7", last_line
  end

  def test_compute
    size = 10000
    content = (0..size).to_a.collect{|num| "Line #{num}" } * "\n"
    TmpFile.with_file(content) do |input_file|
      job = ComputeWorkflow.job(:task2, "TEST", :input_file => input_file)
      io = TSV.get_stream job.run(:stream)
      last_line = nil
      while line = io.gets
        last_line = line.strip
      end
      io.join

      assert_equal "Line #{size}\tTask1\tTask2", last_line
    end
  end

  def test_resume
    Log.severity = 0
    job = ResumeWorkflow.job(:reverse)
    job.recursive_clean
    assert_raise do
      job.run
    end
    assert job.dependencies.first.file('foo').exists?
    assert_equal 'done'.reverse, job.run
  end
end

