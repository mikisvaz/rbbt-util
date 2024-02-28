require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/workflow'
require 'rbbt/hpc/orchestrate/batches'

require_relative '../hpc_test_workflows'

class TestOrchestrateBatches < Test::Unit::TestCase

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
