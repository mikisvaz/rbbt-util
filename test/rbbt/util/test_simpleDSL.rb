require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/simpleDSL'
require 'test/unit'

class TestSimpleDSLClass 
  include SimpleDSL

  def action(name, *args, &block)
    @actions ||= {}
    @actions[name] = args.first
  end

  def actions
    @actions
  end
end


class TestDSL < Test::Unit::TestCase
  def setup 
    @parser = TestSimpleDSLClass.new
    @parser.load_config(:action) do
      action1 "Hello"
      action2 "Good bye"
    end
  end

  def test_actions
    assert_equal({:action1=>"Hello", :action2=>"Good bye"}, @parser.actions)
  end

  def test_method_missing
    assert_raise(NoMethodError){@parser.cues}
  end

  def test_config
    config = <<-EOC
  action1 "Hello"  
  action2 "Good bye" 
    EOC

    begin
      assert_equal config.split("\n").collect{|l| l.strip}, @parser.config(:action).split("\n").collect{|l| l.strip}
    rescue SimpleDSL::NoRuby2Ruby
    end
  end

  def test_parse
    @parser.parse :action do
      action3 "Back again"
    end

    assert_equal({:action1 =>"Hello", :action2 =>"Good bye", :action3 =>"Back again"}, @parser.actions)
  end
end
