require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/task'
Task.basedir = Rbbt.tmp.test.tasks.find :user

class TestTask < Test::Unit::TestCase
  def test_task
    $true
    task = Task.new(:test_task) do $true = true end
    job = task.job(:job1)
    job.start

    assert $true
  end

  def test_task_fork
    TmpFile.with_file do |f|
      task = Task.new(:test_task) do Open.write(f, "Test") end
      job = task.job(:job1)
      job.fork
      job.join

      assert File.exists? f
    end
  end

  def test_task_options
    TmpFile.with_file do |f|
      task = Task.new(:test_task, nil, nil, :name) do |name| Open.write(f, name) end
      job = task.job(:job1, :name => "TestName")
      assert_equal "job1" << "_" << Misc.hash2md5(:name => "TestName"), job.id
      job.fork
      job.join

      assert File.exists? f
      assert_equal "TestName", File.open(f).read
    end
  end

  def test_task_result
    task = Task.new(:test_task, nil, nil, :name) do |name| name end
    job = task.job(:job1, :name => "TestName")
    assert_equal "TestName", job.fork.join.read
  end

  def test_task_info
    task = Task.new(:test_task, nil, nil, :name) do |name| name end
    job = task.job(:job1, :name => "TestName")
    assert_equal "TestName", job.fork.join.info[:options][:name]
  end

  def test_task_status
    task = Task.new(:test_task, nil, nil, :name) do |name| 
      step :one
      name 
    end
    job = task.job(:job1, :name => "TestName")
    assert_equal "TestName", job.fork.join.info[:options][:name]
  end

  def test_task_reopen
    task = Task.new(:test_task, nil, nil, :name) do |name| name end
    job = task.job(:job1, :name => "TestName")
    assert_equal "TestName", job.fork.join.info[:options][:name]
    job = task.job(:job1, :name => "TestName")
    assert_equal "TestName", job.join.info[:options][:name]
  end

  def test_marshal_persistence
    task = Task.new(:test_task, nil, :marshal) do 
      {:a => :b}
    end
    job = task.job(:job1).fork.join
    assert Hash === job.load
    assert_equal :b, job.load[:a]
  end

  def test_yaml_persistence
    task = Task.new(:test_task, nil, :yaml) do 
      {:a => :b}
    end
    job = task.job(:job1).fork.join
    assert Hash === job.load
    assert_equal :b, job.load[:a]
    assert_equal :b, YAML.load(job.open)[:a]
  end

  def test_tsv_persistence
    task = Task.new(:test_task, nil, :tsv) do 
      tsv = TSV.new({})
      tsv.key_field = "A"
      tsv.fields = ["B"]
      tsv.type = :list
      tsv["a"] = ["b"]
      tsv
    end
    job = task.job(:job1).fork.join
    assert TSV === job.load
    assert_equal "b", job.load["a"]["B"]
  end

  def test_dependencies
    task1 = Task.new(:input, nil, :marshal, :value) do |value| value end
    task2 = Task.new(:add_1, nil, :marshal) do input + 1 end
    task2.dependencies << :input
    task3 = Task.new(:times_2, nil, :marshal) do input * 2 end
    task3.dependencies << :add_1
    task4 = Task.new(:times_4, nil, :marshal) do input * 4 end
    task4.dependencies << :add_1

    job = task3.job(:job1, :value => 1)
    assert_equal 4, job.fork.join.load
    job = task4.job(:job1, :value => 1)
    assert_equal 8, job.fork.join.load
  end

 def test_previous_jobs
    task1 = Task.new(:input, nil, :marshal, :value) do |value| value end
    task2 = Task.new(:add_1, nil, :marshal) do input + 1 end
    task2.dependencies << :input
    task3 = Task.new(:times_2, nil, :marshal) do input * 2 + previous_jobs["input"].load end
    task3.dependencies << :add_1
    task4 = Task.new(:times_4, nil, :marshal) do input * 4 + previous_jobs["input"].load end
    task4.dependencies << :add_1

    job = task3.job(:job1, :value => 1)
    assert_equal 5, job.fork.join.load
    job = task4.job(:job1, :value => 1)
    assert_equal 9, job.fork.join.load
  end


end

