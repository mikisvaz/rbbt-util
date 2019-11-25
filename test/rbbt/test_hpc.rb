require File.join(File.expand_path(File.dirname(__FILE__)), '..', 'test_helper.rb')
require 'rbbt/hpc'
require 'rbbt/workflow'

class TestHPC < Test::Unit::TestSuite
  def test_relay
    Log.severity = 0
    Workflow.require_workflow "Translation"
    job = Translation.job(:translate, nil, :genes => %w(TP53 KRAS))
    Marenostrum.relay job

  end
end

