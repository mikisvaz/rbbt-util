require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/workflow'
require 'rbbt/workflow/soap'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestSOAPWF
  extend Workflow

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

  export_synchronous :double
  export_asynchronous :repeat2
end

TestSOAPWF.workdir = Rbbt.tmp.test.workflow

class TestSOAP < Test::Unit::TestCase

  def setup
    port= 77177
    $server = WorkflowSOAP.new TestSOAPWF, "Test WF", 'localhost', port
    Thread.new do
      $server.start
    end
    Open.write(Rbbt.tmp.test.wsdl.TestWS.find(:user), $server.wsdl)

    $driver = SimpleWS.get_driver('http://localhost:' + port.to_s, "TestSOAPWF")
  end

  def teardown
    $server.shutdown
    FileUtils.rm_rf(Rbbt.tmp.test.wsdl.TestWS.find(:user))
  end

  def test_wsdl
    assert $server.wsdl =~ /jobid/
  end

  def test_check_status
    str = "TEST"
    job = TestSOAPWF.job(:repeat2, nil, :number => 3)
    id = TestSOAPWF.id_for job.path
    job.run
    assert $driver.done id
  end

  def test_launch_job
    str = "TEST"
    id = $driver.repeat2("Default", 3)
    while not $driver.done id
      sleep 1
    end
    assert $driver.status(id) == "done"
  end

  def test_load_job
    str = "TEST"
    id = $driver.repeat2("Default", 3)
    $driver.clean id
    id = $driver.repeat2("Default", 3)
    while not $driver.done id
      sleep 1
    end
    assert_equal ["TEST"] * 6 * "\n", $driver.load_string(id)
  end

  def test_synchronous
    assert_equal 6, $driver.double(3)
  end
end
