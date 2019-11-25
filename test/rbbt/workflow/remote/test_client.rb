require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/remote/client'

class TestRemote < Test::Unit::TestCase
  def test_ssh
    Log.severity = 0
    client = WorkflowRemoteClient.new "ssh://localhost:Translation", "Translation"
    job = client.job("translate", "SSH-TEST", :genes => ["TP53","KRAS"])
    iii job.url
    puts job.run
  end
end

