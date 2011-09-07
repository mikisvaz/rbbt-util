require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow/task'
require 'rbbt'

class TEST
  def self.times(str, times)
    [str] * times 
  end
end

class TestTask < Test::Unit::TestCase
  def test_task_setup
    a = [1,2]
    p = Task.setup{|pos| self[pos]}

    assert_equal 1, p.exec_in(a, 0)
    assert_equal 2, p.exec_in(a, 1)
  end

  def test_task_exec
    a = [1,2]
    p = Proc.new{|pos| self[pos]}
    p.extend Task
    assert_equal 1, p.exec_in(a, 0)
    assert_equal 2, p.exec_in(a, 1)
  end

  def test_task_named
    t = Task.setup :inputs => %w(str times) do |str, times| [str] * times end
    assert_equal %w(test test), t.exec(:str => "test", :times => 2)
    assert_equal %w(test test), t.exec(:str => "test", :times => 2)
  end

  def test_task_persist
    t = Task.setup :inputs => %w(str times) do |str, times| 
      [str] * times 
    end

    TmpFile.with_file do |perfile|
      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
      assert File.exist? perfile

      t = Task.setup :inputs => %w(str times) do |str, times| 
        raise "Persistence ignored"
      end

      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
    end
  end

  def test_task_unbound_method
    t = TEST.method :times
    t.extend Task
    t.inputs = %w(str times) 

    TmpFile.with_file do |perfile|
      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
      assert File.exist? perfile

      t = Task.setup :inputs => %w(str times) do |str, times| 
        raise "Persistence ignored"
      end

      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
    end
  end

  def test_task_unbound_method2
    t = Task.setup :inputs => %w(str times), &TEST.method(:times)

    TmpFile.with_file do |perfile|
      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
      assert File.exist? perfile

      t = Task.setup :inputs => %w(str times) do |str, times| 
        raise "Persistence ignored"
      end

      assert_equal %w(test test), t.persist_exec_in(perfile, Object.new, :str => "test", :times => 2)
    end
  end


end
