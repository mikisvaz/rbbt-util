require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow'
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/tsv'
require 'rbbt'
require 'rbbt-util'

class TestStep < Test::Unit::TestCase

  def test_step
    task  = Task.setup do "TEST" end
    task2  = Task.setup do raise "Persistence ignored" end
    TmpFile.with_file do |tmp|
      step = Step.new tmp, task
      assert_equal "TEST", step.run
      assert File.exist? tmp
      step = Step.new tmp, task2
      assert_equal "TEST", step.run
    end
  end

  def test_dependency
    str = "TEST"
    str2 = "TEST2"
    TmpFile.with_file do |tmpfile|

      task1  = Task.setup :result_type => :string do 
        Open.write(tmpfile, str); 
        "done"
      end
      step1 = Step.new tmpfile + 'step1', task1

      task2  = Task.setup :result_type => :string do 
        Open.read(tmpfile) 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], [step1]

      step2.run
      assert_equal "TEST", Open.read(tmpfile + 'step2')

      task2  = Task.setup :result_type => :string do 
        str2 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], step1

      step1.clean
      step2.clean.run
      assert_equal "TEST2", Open.read(tmpfile + 'step2')
    end
  end

  def __test_dependency_log_relay
    str = "TEST"
    TmpFile.with_file do |tmpfile|
      task1  = Task.setup :result_type => :string, :name => :task1 do 
        log(:starting_task1, "Starting Task1")
        Open.write(tmpfile, str); 
        "done"
      end
      step1 = Step.new tmpfile + 'step1', task1

      task2  = Task.setup :result_type => :string, :name => :task1 do 
        Open.read(tmpfile) 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], [step1]

      step2.run
      assert step2.messages.include? "Starting Task1"
    end
  end

  def test_log_relay_step
    str = "TEST"
    TmpFile.with_file do |tmpfile|
      task1  = Task.setup :result_type => :string, :name => :task1 do 
        log(:starting_task1, "Starting Task1")
        Open.write(tmpfile, str); 
        "done"
      end
      step1 = Step.new tmpfile + 'step1', task1


      task2  = Task.setup :result_type => :string, :name => :task1 do 
        Open.read(tmpfile) 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], [step1]

      Step.log_relay_step = step2
      step2.run

      assert step2.messages.include? "Starting Task1"
    end
  end


  def test_exec
    TmpFile.with_file do |lock|
      task  = Task.setup do "TEST" end
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        assert_equal "TEST", step.exec
      end
    end
  end


  def __test_fork
    TmpFile.with_file do |lock|
      task  = Task.setup do while not File.exist?(lock) do sleep 1; end; "TEST" end
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        job = step.fork
        assert !job.done?
        assert_raise RuntimeError do step.fork end
        sleep 1
        Open.write(lock, "open")
        assert_equal "TEST", job.join.load
        assert job.done?
      end
    end
  end

  def __test_abort
    TmpFile.with_file do |lock|
      task  = Task.setup do while not File.exist?(lock) do sleep 1; end; "TEST" end
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        job = step.fork
        assert !job.done?
        step.clean.fork 
        job.abort 
        assert_equal :aborted, job.status
        Open.write(lock, "open")
        job.clean.fork 
        job.join
        assert job.done?
      end
    end
  end

  def test_files
    TmpFile.with_file do |lock|
      task  = Task.setup do 
        Open.write(file("test"),"TEST")
      end
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        #job = step.fork
        #while not job.done? do sleep 1 end
        step.run
        assert_equal "TEST", Open.read(step.file("test"))
      end
    end
  end

  def test_messages
    TmpFile.with_file do |lock|

      task  = Task.setup do 
        message "WRITE"
        Open.write(file("test"),"TEST")
        Open.write(path,"done")
        nil
      end

      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        job = step
        step.run
        while not job.done? do sleep 1 end
        assert_equal "TEST", Open.read(job.file("test"))
        assert job.messages.include? "WRITE"
      end
    end
  end

  def test_subdir
    TmpFile.with_file do |lock|
      task  = Task.setup do 
        message "WRITE"
        Open.write(file("test"),"TEST")
      end

      TmpFile.with_file do |tmp|
        step = Step.new File.join(tmp, 'subdir1', 'subdir2'), task
        #job = step.fork
        #while not job.done? do sleep 1 end
        step.run
        assert_equal "TEST", Open.read(step.file("test"))
        assert step.messages.include? "WRITE"
      end
    end
  end

  def test_semaphore
    TmpFile.with_file do |semaphore|
      begin
        semaphore = "/" << semaphore.gsub('/','_')
        RbbtSemaphore.create_semaphore(semaphore, 2)

        task  = Task.setup do 
          5.times do
            puts "Process: #{Process.pid}"
            sleep rand 
          end
        end

        jobs = []
        10.times do
          TmpFile.with_file do |tmp|
            step = Step.new tmp, task
            jobs << step.fork(semaphore)
          end
        end
        Step.wait_for_jobs(jobs)
      ensure
        RbbtSemaphore.delete_semaphore(semaphore)
      end
    end
  end

  def __test_load_return_description
    require 'rbbt/workflow'
    Workflow.require_workflow "Study"
    study = Study.setup("LICA-FR")
    job = study.recurrent_mutations(:job)
    iii job.load.organism
    iii study.recurrent_mutations.organism
  end
end
