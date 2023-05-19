require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/util/orchestrator'
require 'rbbt/workflow/util/trace'
require 'rbbt-util'
require 'rbbt/workflow'

class TestClass < Test::Unit::TestCase
  setup do
    module TestWF
      extend Workflow 

      MULT = 0.1
      task :a => :text do
        sleep(TestWF::MULT * (rand(10) + 2))
      end

      dep :a
      task :b => :text do
        sleep(TestWF::MULT * (rand(10) + 2))
      end

      dep :a
      dep :b
      task :c => :text do
        sleep(TestWF::MULT * (rand(10) + 2))
      end

      dep :c
      task :d => :text do
        sleep(TestWF::MULT * (rand(10) + 2))
      end

      dep :c
      task :e => :text do
        sleep(TestWF::MULT * (rand(10) + 2))
      end
    end

  end
  def test_orchestrate_resources

    jobs =[]

    num = 10
    num.times do |i|
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:d, name + " #{i}") }
    end
    jobs.each do |j| j.recursive_clean end

    rules = YAML.load <<-EOF
defaults:
  log: 4
default_resources:
  IO: 1
TestWF:
  a:
    resources:
      cpus: 7
  b:
    resources:
      cpus: 2
  c:
    resources:
      cpus: 10
  d:
    resources:
      cpus: 15
    EOF

    orchestrator = Workflow::Orchestrator.new(TestWF::MULT, "cpus" => 30, "IO" => 4, "size" => 10 )
    Log.with_severity 0 do
      orchestrator.process(rules, jobs)
    end

    data = Workflow.trace jobs, :plot_data => true
    eend = data.column("End.second").values.collect{|v| v.to_f}.max
    second_cpus = TSV.setup({}, "Second~CPUS#:type=:single#:cast=:to_f")
    (0..eend.to_i).each do |second|
      tasks = data.select("Start.second"){|s| s <= second}.select("End.second"){|s| s > second}
      cpus = 0
      tasks.through :key, ["Workflow", "Task"] do |k, values|
        workflow, task = values
        cpus += rules[workflow][task.to_s]["resources"]["cpus"]
      end
      second_cpus[second] = cpus
    end

    assert Misc.mean(second_cpus.values) > 15
    assert Misc.mean(second_cpus.values) < 30
  end

  def test_orchestrate_erase

    jobs =[]

    num = 10
    num.times do |i|
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:d, name + " #{i}") }
    end
    jobs.each do |j| j.recursive_clean end

    rules = YAML.load <<-EOF
defaults:
  log: 4
default_resources:
  IO: 1
TestWF:
  a:
    erase: true
    resources:
      cpus: 7
  b:
    erase: true
    resources:
      cpus: 2
  c:
    resources:
      cpus: 10
  d:
    resources:
      cpus: 15
    EOF

    orchestrator = Workflow::Orchestrator.new(TestWF::MULT, "cpus" => 30, "IO" => 4, "size" => 10 )
    Log.with_severity 3 do
      orchestrator.process(rules, jobs)
    end

    jobs.each do |job|
      assert job.step(:c).dependencies.empty?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/a/")}.any?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/b/")}.any?
    end

  end

  def test_orchestrate_default

    jobs =[]

    num = 3
    num.times do |i|
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:d, name + " #{i}") }
    end
    jobs.each do |j| j.recursive_clean end

    rules = YAML.load <<-EOF
defaults:
  erase: true
  log: 4
default_resources:
  IO: 1
TestWF:
  a:
    erase: true
    resources:
      cpus: 7
  b:
    erase: true
    resources:
      cpus: 2
  c:
    erase: false
    resources:
      cpus: 10
  d:
    resources:
      cpus: 15
    EOF

    orchestrator = Workflow::Orchestrator.new(TestWF::MULT, "cpus" => 30, "IO" => 4, "size" => 10 )
    Log.with_severity 3 do
      orchestrator.process(rules, jobs)
    end

    jobs.each do |job|
      assert job.step(:c).dependencies.empty?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/a/")}.any?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/b/")}.any?
    end

  end

  def test_orchestrate_top_level

    jobs =[]

    num = 3
    num.times do |i|
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:d, name + " #{i}") }
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:c, name + " #{i}") }
    end
    jobs.each do |j| j.recursive_clean end

    rules = YAML.load <<-EOF
defaults:
  erase: true
  log: 4
default_resources:
  IO: 1
TestWF:
  a:
    resources:
      cpus: 7
  b:
    resources:
      cpus: 2
  c:
    resources:
      cpus: 10
  d:
    resources:
      cpus: 15
    EOF

    orchestrator = Workflow::Orchestrator.new(TestWF::MULT, "cpus" => 30, "IO" => 4, "size" => 10 )
    Log.with_severity 3 do
      orchestrator.process(rules, jobs)
    end

    jobs.each do |job|
      next unless job.task_name.to_s == 'd'
      assert job.step(:c).dependencies.empty?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/a/")}.any?
      assert job.step(:c).info[:archived_info].keys.select{|k| k.include?("TestWF/b/")}.any?
    end

  end

  def test_orchestrate_top_level_double_dep

    jobs =[]

    num = 10
    num.times do |i|
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:e, name + " #{i}") }
      jobs.concat %w(TEST1 TEST2).collect{|name| TestWF.job(:d, name + " #{i}") }
    end
    jobs.each do |j| j.recursive_clean end

    rules = YAML.load <<-EOF
defaults:
  erase: true
  log: 4
default_resources:
  IO: 1
TestWF:
  a:
    resources:
      cpus: 7
  b:
    resources:
      cpus: 2
  c:
    resources:
      cpus: 10
  d:
    resources:
      cpus: 15
    EOF

    orchestrator = Workflow::Orchestrator.new(TestWF::MULT, "cpus" => 30, "IO" => 4, "size" => 10 )
    Log.with_severity 3 do
      orchestrator.process(rules, jobs)
    end

    jobs.each do |job|
      next unless job.task_name.to_s == 'd' || job.task_name.to_s == 'e'
      assert job.info[:archived_info].keys.select{|k| k.include?("TestWF/c/")}.any?
      assert job.info[:archived_info].keys.select{|k| k.include?("TestWF/c/")}.any?
    end

  end
end

