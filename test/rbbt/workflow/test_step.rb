require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow/task'
require 'rbbt/workflow/step'
require 'rbbt/tsv'
require 'rbbt'

class TestStep < Test::Unit::TestCase

  def test_step
    task  = Task.setup do "TEST" end
    task2  = Task.setup do raise "Persistence ignored" end
    TmpFile.with_file do |tmp|
      step = Step.new tmp, task
      assert_equal "TEST", step.run
      assert File.exists? tmp
      step = Step.new tmp, task2
      assert_equal "TEST", step.run
    end
  end

  def test_dependency
    str = "TEST"
    str2 = "TEST2"
    TmpFile.with_file do |tmpfile|

      task1  = Task.setup :result_type => nil do 
        Open.write(tmpfile, str); 
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

  def test_dependency_log_relay
    str = "TEST"
    TmpFile.with_file do |tmpfile|
      task1  = Task.setup :result_type => nil, :name => :task1 do 
        log(:starting_task1, "Starting Task1")
        Open.write(tmpfile, str); 
      end
      step1 = Step.new tmpfile + 'step1', task1

      task2  = Task.setup :result_type => :string, :name => :task1 do 
        Open.read(tmpfile) 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], [step1]

      step2.run
      assert step2.messages.include? "task1>Starting Task1"
    end
  end

  def test_log_relay_step
    str = "TEST"
    TmpFile.with_file do |tmpfile|
      task1  = Task.setup :result_type => nil, :name => :task1 do 
        log(:starting_task1, "Starting Task1")
        Open.write(tmpfile, str); 
      end
      step1 = Step.new tmpfile + 'step1', task1


      task2  = Task.setup :result_type => :string, :name => :task1 do 
        Open.read(tmpfile) 
      end
      step2 = Step.new tmpfile + 'step2', task2, [], [step1]

      Step.log_relay_step = step2
      step2.run
      assert step2.messages.include? "task1>Starting Task1"
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


  def test_fork
    TmpFile.with_file do |lock|
      task  = Task.setup do while not File.exists?(lock) do sleep 1; end; "TEST" end
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

  def test_abort
    TmpFile.with_file do |lock|
      task  = Task.setup do while not File.exists?(lock) do sleep 1; end; "TEST" end
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        job = step.fork
        assert !job.done?
        assert_raise RuntimeError do step.fork end
        sleep 1
        while not job.abort do sleep 1 end
        Open.write(lock, "open")
        job.join
        assert job.aborted?
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
        job = step.fork
        while not job.done? do sleep 1 end
        assert_equal "TEST", Open.read(job.file("test"))
      end
    end
  end

  def test_messages
    TmpFile.with_file do |lock|
      task  = Task.setup do 
        message "WRITE"
        Open.write(file("test"),"TEST")
      end
      
      TmpFile.with_file do |tmp|
        step = Step.new tmp, task
        job = step
        step.run
        while not job.done? do sleep 1 end
        assert_equal "TEST", Open.read(job.file("test"))
        assert_equal "WRITE", job.messages.last
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
        job = step.fork
        while not job.done? do sleep 1 end
        assert_equal "TEST", Open.read(job.file("test"))
        assert_equal "WRITE", job.messages.last
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
            sleep rand * 2
          end
        end

        jobs = []
        10.times do
          TmpFile.with_file do |tmp|
            step = Step.new tmp, task
            jobs << step.fork(semaphore)
          end
        end
        jobs.each do |job|
          while not job.done?
            sleep 1
          end
        end
      ensure
        RbbtSemaphore.delete_semaphore(semaphore)
      end
    end

  end

end
