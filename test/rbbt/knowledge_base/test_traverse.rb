require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/knowledge_base/traverse'
require 'rbbt/workflow'

Workflow.require_workflow "Genomics"
class TestKnowledgeBaseTraverse < Test::Unit::TestCase
  def kb
    Genomics.knowledge_base
  end

  def _test_traverse
    rules = []
    rules << "?1 pina SF3B1 - Method=MI:0006"
    rules << "TP53 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    iii res
    assert res.first.include? "?1"
  end

  def _test_path
    rules = []
    rules << "?1 pina ARPC2"
    rules << "ARPC3 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
    iii res.last.first
  end

  def test_path2
    rules = []
    rules << "?1 pina SF3B1"
    rules << "?2 pina SF3B1"
    rules << "?1 pina ?2"
    res =  kb.traverse rules
    assert res.first.include? "?1"
    iii res.last.first.first.source
  end
end

