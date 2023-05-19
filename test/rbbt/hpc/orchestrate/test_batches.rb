require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'scout/workflow'
require 'rbbt/hpc/orchestrate/batches'

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

  dep_task :c4, TestWFC, :c3
end

module TestWFD
  extend Workflow

  dep TestWFC, :c3, :jobname => "First c3"
  dep TestWFC, :c3, :jobname => "Second c3"
  task :d1 => :string do self.task_name.to_s end
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
  chain_a_b:
    tasks: TestWFB#b1, TestWFB#b2, TestWFA#a1, TestWFA#a2
    config_keys: key2 value2 token2, key3 value3 token3.1 token3.2
  chain_a:
    workflow: TestWFA
    tasks: a1, a2, a3
    config_keys: key2 value2 token2, key3 value3 token3.1 token3.2
  chain_b:
    workflow: TestWFB
    tasks: b1, b2
  chain_b2:
    tasks: TestWFB#b1, TestWFB#b2, TestWFA#a1
  chain_d:
    tasks: TestWFD#d1, TestWFC#c1, TestWFC#c2, TestWFC#c3
TestWFA:
  defaults:
    log: 4
    config_keys: key4 value4 token4
    time: 10min
  a1:
    cpus: 10
    config_keys: key5 value5 token5
TestWFC:
  defaults:
    skip: true
    log: 4
    time: 10s
  EOF


  def test_job_batches_d
    job = TestWFD.job(:d1, nil)
    job.recursive_clean

    batches = HPC::Orchestration.job_batches(RULES, job)
    assert_equal 3, batches.length
  end

  def test_job_batches_c3
    job = TestWFC.job(:c3, nil)
    job.recursive_clean

    batches = HPC::Orchestration.job_batches(RULES, job)
  end

  def test_job_batches_c4
    job = TestWFC.job(:c4, nil)
    job.recursive_clean

    batches = HPC::Orchestration.job_batches(RULES, job)
    assert_equal 3, batches.length
  end

end
