require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow'

class TestWorkflowDoc < Test::Unit::TestCase
  TEMPLATE=<<-EOF
Title of the template

Paragraph1

Paragraph2

# Tasks

Paragraph3

## task1

Task 1

## task2

Task 2

  EOF
  def test_parse
    ddd Workflow.parse_workflow_doc(TEMPLATE)
  end
end

