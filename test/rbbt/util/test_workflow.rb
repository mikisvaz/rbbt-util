require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt'
require 'rbbt/util/tsv'
require 'rbbt/util/tmpfile'
require 'rbbt/util/workflow'

module WF
  include WorkFlow

  stage_options :option1 => :string
  stage_option_defaults :option1 => "hola"
  stage :stage1 do |option1|

  end
end

class TestWorkflow < Test::Unit::TestCase
  def test_stage_name
    assert Stage.name(:stage1, "jobname1", {:option => "value"}) =~ /\.\/stage1\/jobname1_/
    Stage.basedir = "/tmp/stages"
    assert Stage.name(:stage1, "jobname1", {:option => "value"}) =~ /\/tmp\/stages\/stage1\/jobname1_/
  end
end
