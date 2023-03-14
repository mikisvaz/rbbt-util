require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/knowledge_base/traverse'
require 'rbbt/workflow'

class TestKnowledgeBaseTraverse < Test::Unit::TestCase
  def setup
    Workflow.require_workflow "Genomics"
  end

  def kb
    Genomics.knowledge_base
  end

  def test_traverse
    rules = []
    rules << "?1 pina SF3B1 - Method=MI:0006"
    rules << "TP53 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end

  def test_traverse2
    rules = []
    rules << "?target =pina SF3B1"
    rules << "?1 pina ?target - Method=MI:0006"
    rules << "TP53 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end

  def test_traverse3
    rules = []
    rules << "?target = ENSG00000115524"
    rules << "?1 pina ?target - Method=MI:0006"
    rules << "TP53 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end


  def test_traverse_acc
    Log.severity = 0
    rules_str=<<-EOF
?target{
  ?target pina SF3B1
}
?1 pina TP53
?1 pina ?target
    EOF
    rules = rules_str.split "\n"
    res =  kb.traverse rules
    iii res
    assert res.first.include? "?1"
  end


  def test_path
    rules = []
    rules << "?1 pina ARPC2"
    rules << "ARPC3 pina ?2"
    rules << "?2 pina ?1"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end

  def test_path2
    rules = []
    rules << "?1 pina SF3B1"
    rules << "?2 pina SF3B1"
    rules << "?1 pina ?2"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end

  def test_wildcard_db
    rules = []
    rules << "?1 ?db SF3B1 - Method=MI:0006"
    res =  kb.traverse rules
    assert res.first.include? "?1"
  end
end

