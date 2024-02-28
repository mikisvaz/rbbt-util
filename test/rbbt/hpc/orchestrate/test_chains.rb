require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/hpc/orchestrate/chains'

require_relative '../hpc_test_workflows'

class TestOrchestrateChains < Test::Unit::TestCase

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
    comment: Should not include TestWFA#a1
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

  def test_job_chain_a
    job = TestWFA.job(:a3, nil)
    job.recursive_clean

    job_chains = HPC::Orchestration.job_chains(RULES, job)
    job_chains = NamedArray.setup(job_chains, job_chains.collect{|n,i| n })

    assert_equal %w(chain_a_b chain_a), job_chains.fields
    assert_equal 3, job_chains["chain_a"].last[:jobs].length
    assert_equal job, job_chains["chain_a"].last[:top_level]
  end

  def test_job_chain_b
    job = TestWFB.job(:b2, nil)
    job.recursive_clean

    job_chains = HPC::Orchestration.job_chains(RULES, job)
    job_chains = NamedArray.setup(job_chains, job_chains.collect{|n,i| n })

    assert_equal %w(chain_a chain_b chain_b2 chain_a_b).sort, job_chains.fields.sort

    assert_equal 2, job_chains["chain_a"].last[:jobs].length
    assert_equal job.step("a2"), job_chains["chain_a"].last[:top_level]

    assert_equal 2, job_chains["chain_b"].last[:jobs].length
    assert_equal job, job_chains["chain_b"].last[:top_level]

    assert_equal 2, job_chains["chain_b2"].last[:jobs].length
    assert_equal job, job_chains["chain_b2"].last[:top_level]

    assert_equal 4, job_chains["chain_a_b"].last[:jobs].length
    assert_equal job, job_chains["chain_a_b"].last[:top_level]
  end
  
  def test_job_chains_double
    job = TestWFD.job(:d1, nil)
    job.recursive_clean

    job_chains = HPC::Orchestration.job_chains(RULES, job)
    job_chains = NamedArray.setup(job_chains, job_chains.collect{|n,i| n })

    assert_equal 2, job_chains.select{|n,i| n == 'chain_a' }.length
    assert_equal ["First c3", "Second c3"].sort, job_chains.
      select{|n,i| n == 'chain_a' }.
      collect{|n,i| i[:top_level].name }.sort

    assert_equal 1, job_chains.select{|n,i| n == 'chain_d' }.length
  end

  def __test_benchmark

    job = TestDeepWF.job(:suite, :size => 100)
    job.recursive_clean

    Misc.benchmark(10) do
      HPC::Orchestration.job_chains(RULES, job)
    end

  end
end

