require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow/util/orchestrator'
require 'rbbt/workflow/util/trace'
require 'rbbt-util'
require 'rbbt/workflow'

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
  
  dep :b
  task :c => :text do
    sleep(TestWF::MULT * (rand(10) + 2))
  end
  
  dep :c
  task :d => :text do
    sleep(TestWF::MULT * (rand(10) + 2))
  end
end

class TestClass < Test::Unit::TestCase
  def test_orchestrate

    jobs =[]

    num = 10
    num.times do |i|
      jobs.concat %w(test1 test2).collect{|name| TestWF.job(:d, name + " #{i}") }
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
end

