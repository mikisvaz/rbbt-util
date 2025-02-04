
require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/workflow'
require 'rbbt/hpc/orchestrate'

module TestWFA
  extend Workflow

  task :a1 => :string do self.task_name.to_s end

  dep :a1
  task :a2 => :string do self.task_name.to_s end

  dep :a2
  task :a3 => :string do self.task_name.to_s end
end

module TestWFB
  extend Workflow

  dep TestWFA, :a2
  task :b1 => :string do self.task_name.to_s end

  dep :b1
  task :b2 => :string do self.task_name.to_s end
end

module TestWFC
  extend Workflow

  dep TestWFA, :a1
  dep_task :c1, TestWFB, :b2

  task :c2 => :string do self.task_name.to_s end

  dep :c1
  dep :c2
  task :c3 => :string do self.task_name.to_s end
end

class TestOrchestrate < Test::Unit::TestCase

  RULES = IndiferentHash.setup(YAML.load(<<-EOF))
---
defaults:
  queue: first_queue
  time: 1h
  log: 2
  config_keys: key1 value1 token1
chains:
  chain_a:
    workflow: TestWFA
    tasks: a1, a2, a3
    config_keys: key2 value2 token2, key3 value3 token3.1 token3.2
  chain_b:
    workflow: TestWFB
    tasks: b1, b2
  chain_b2:
    comment: This chain is not valid, it is missing a2
    tasks: TestWFA#a1, TestWFA#a3, TestWFB#b1, TestWFB#b2
TestWFA:
  defaults:
    log: 4
    config_keys: key4 value4 token4
  a1:
    cpus: 10
    config_keys: key5 value5 token5
TestWFC:
  defaults:
    skip: true
    log: 4
  EOF

  def test_parse_chains
    chains = HPC::Orchestration.parse_chains RULES
    assert chains[:chain_a][:tasks]["TestWFA"].include?("a1")
  end

  def test_task_options
    rules = HPC::Orchestration.task_specific_rules RULES, "TestWFA", :a1
    assert_equal 10, rules[:cpus]
    assert_equal 4, rules[:log]
  end

  def test_job_workload
    job = TestWFB.job(:b2, nil)
    job.recursive_clean

    workload = HPC::Orchestration.job_workload(job)

    assert workload.include? job
  end

  def test_job_chains
    job = TestWFB.job(:b2, nil)

    job.recursive_clean
    job_chains = HPC::Orchestration.job_chains(RULES, job)
    assert_equal 3, job_chains.length

    TestWFB.job(:b1).run
    job_chains = HPC::Orchestration.job_chains(RULES, job)
    assert_equal 0, job_chains.length

    job.recursive_clean
    TestWFA.job(:a1).run
    job_chains = HPC::Orchestration.job_chains(RULES, job)
    assert_equal 2, job_chains.length
  end

  def test_job_batches
    job = TestWFB.job(:b2, nil)

    job.recursive_clean
    batches = HPC::Orchestration.job_batches(RULES, job)
    assert_equal 2, batches.length

    assert_equal job.rec_dependencies.length + 1, batches.inject(0){|acc,e| acc += e[:jobs].length }
  end

  def test_job_batches_skip
    job = TestWFC.job(:c1, nil)

    job.recursive_clean
    batches = HPC::Orchestration.job_batches(RULES, job)
    assert_equal 2, batches.length

    assert_equal job.rec_dependencies.length + 1, batches.inject(0){|acc,e| acc += e[:jobs].length }
  end

end

