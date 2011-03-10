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
      task = Task.new(:test_task, nil, :name) do |name| Open.write(f, name) end
      job = task.job(:job1, :name => "TestName")
      assert_equal "job1" << "_" << Misc.hash2md5(:name => "TestName"), job.id
      job.fork
      job.join

      assert File.exists? f
      assert_equal "TestName", File.open(f).read
    end
  end

  def test_task_result
    TmpFile.with_file do |f|
      task = Task.new(:test_task, nil, :name) do |name| name end
      job = task.job(:job1, :name => "TestName")
      assert_equal "TestName", job.fork.join.read
    end
  end

  def test_task_info
    TmpFile.with_file do |f|
      task = Task.new(:test_task, nil, :name) do |name| name end
      job = task.job(:job1, :name => "TestName")
      assert_equal "TestName", job.fork.join.info[:options][:name]
    end
  end

  def test_task_status
    TmpFile.with_file do |f|
      task = Task.new(:test_task, nil, :name) do |name| 
        step :one
        name 
      end
      job = task.job(:job1, :name => "TestName")
      assert_equal "TestName", job.fork.join.info[:options][:name]
    end
  end

  def test_task_reopen
    TmpFile.with_file do |f|
      task = Task.new(:test_task, nil, :name) do |name| name end
      job = task.job(:job1, :name => "TestName")
      assert_equal "TestName", job.fork.join.info[:options][:name]
      job = task.job(:job1, :name => "TestName")
      assert_equal "TestName", job.join.info[:options][:name]
    end
  end


end

