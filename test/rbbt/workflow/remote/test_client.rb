require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow'
require 'rbbt/workflow/remote/client'

class TestWFRest
  extend Workflow

  input :name, :string, "Name to greet", "World"
  task :hi => :string do |name|
    "Hello #{name}"
  end

  dep :hi
  task :intro => :string do 
    step(:hi).load + ", nice to meet you"
  end
end

class TestRemote < Test::Unit::TestCase

  def _test_ssh
    Log.severity = 0
    client = WorkflowRemoteClient.new "ssh://localhost:Translation", "Translation"
    job = client.job("translate", "SSH-TEST", :genes => ["TP53","KRAS"])
    iii job.url
    puts job.run
  end

  def test_rest
    Log.with_severity 0 do

      workflow_server(TestWFRest) do |client|
        assert_equal "Hello World", client.job(:hi, nil, {}).run
        assert_equal "Hello Miguel", client.job(:hi, nil, {:name => :Miguel}).run
        assert_equal "Hello Miguel, nice to meet you", client.job(:intro, nil, {:name => :Miguel}).run
      end

      workflow_server(TestWFRest, :Port => 1902) do |client|
        assert_equal "Hello World", client.job(:hi, nil, {}).run
        assert_equal "Hello Miguel", client.job(:hi, nil, {:name => :Miguel}).run
        assert_equal "Hello Miguel, nice to meet you", client.job(:intro, nil, {:name => :Miguel}).run
      end
    end
  end

  def test_rest_clean
    real_job = TestWFRest.job(:hi)
    real_job.run
    first_time = File.ctime(real_job.path)
    workflow_server(TestWFRest) do |client|
      assert File.ctime(real_job.path) == first_time
      assert_equal "Hello World", client.job(:hi, nil, {}).clean.run
      assert File.ctime(real_job.path) > first_time
    end
  end
end

