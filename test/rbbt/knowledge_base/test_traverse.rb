require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/knowledge_base/traverse'
require 'rbbt/workflow'

Workflow.require_workflow "Genomics"
class TestKnowledgeBaseTraverse < Test::Unit::TestCase
  def kb
    Genomics.knowledge_base
  end

  def test_traverse
    rules = []
    rules << "SF3B1 pina ?1 - Method=MI:0006"
    rules << "TP53 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end
end

