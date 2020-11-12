require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow'
require 'rbbt/workflow/remote_workflow'

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

class TestRemoteWorkflow < Test::Unit::TestCase

  def remote_workflow_server(workflow, options = {}, &block)
    trap(:USR1){ raise TestServerLoaded}

    begin
      pid = Process.fork do 
        TmpFile.with_file do |app_dir|
          Misc.in_dir(app_dir) do
            require 'rack'
            ENV["RBBT_WORKFLOW_EXPORT_ALL"] = 'true'

            app_dir = Path.setup(app_dir.dup)
            Open.write(app_dir.etc.target_workflow.find, workflow.to_s)

            config_ru_file = File.exist?('./workflow_config.ru') ? './workflow_config.ru' : Rbbt.share['workflow_config.ru'].find
            options[:config] = config_ru_file
            app = Rack::Server.new(options)
            app.start do
              Process.kill :USR1, Process.ppid
            end
          end
        end
      end

      begin
        sleep 1 while true
      rescue TestServerLoaded
      end

      client = RemoteWorkflow.new "http://localhost:#{options[:Port] || 9292}/#{workflow.to_s}", workflow.to_s

      yield client

    rescue
      Log.exception $!
    ensure
      Process.kill :INT, pid
      Process.wait pid
    end
  end

  def test_rest
    Log.with_severity 0 do

      remote_workflow_server(TestWFRest) do |client|
        job = client.job(:hi, nil, {})
        job.clean
        job = client.job(:hi, nil, {})
        assert ! job.done?
        job.run
        job.produce
        job = client.job(:hi, nil, {})
        assert job.done?
        sleep 1
      end

      remote_workflow_server(TestWFRest) do |client|
        assert_equal "Hello World", client.job(:hi, nil, {}).run.chomp
        assert_equal "Hello Miguel", client.job(:hi, nil, {:name => :Miguel}).run.chomp
        assert_equal "Hello Miguel, nice to meet you", client.job(:intro, nil, {:name => :Miguel}).run.chomp
      end

      remote_workflow_server(TestWFRest, :Port => 1902) do |client|
        assert_equal "Hello World", client.job(:hi, nil, {}).run.chomp
        assert_equal "Hello Miguel", client.job(:hi, nil, {:name => :Miguel}).run.chomp
        assert_equal "Hello Miguel, nice to meet you", client.job(:intro, nil, {:name => :Miguel}).run.chomp
      end
    end
  end


  def test_ssh
    Log.severity = 0
    client = RemoteWorkflow.new "ssh://#{ENV["HOSTNAME"]}:Translation", "Translation"
    job = client.job("translate", "SSH-TEST-1", :genes => ["TP53","KRAS"])
    assert_equal 2, job.run.select{|l| l =~ /ENSG/}.length
  end
end

