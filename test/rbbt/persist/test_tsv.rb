require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/persist'
require 'rbbt/annotations'
require 'rbbt/util/tmpfile'
require 'test/unit'



class TestPersistTSV < Test::Unit::TestCase
  MAX = 100_002

  def tsv_path
    Random.new
    require 'rbbt/workflow'
    Workflow.require_workflow "Genomes1000"
    require 'rbbt/sources/genomes1000'
    CMD.cmd("grep -v '^\\.\t' | head -n #{MAX}", :in => Genomes1000.mutations.open, :pipe => true)
  end

  def run_bechmark(file, engine)
    Log.info "Testing #{ Term::ANSIColor.red(engine) }"
    TmpFile.with_file nil, false do |tmp_file|
      db= nil

      Misc.benchmark(1, "Build database with #{MAX - 2} entries") do
        db = TSV.open(file, :fields => [1], :persist => true, :persist_engine => engine, :persist_dir => tmp_file, :type => :single, :unnamed => true)
      end

      test = db.keys.sort{rand}[1..100000]
      Misc.benchmark(5, "Access #{test.length} random entries") do
        test.each do |k| db[k] end
      end
      Log.info "Profiling access to #{test.length} random entries"
      Misc.profile :min_percent => 0.1 do
        test.each do |k| db[k] end
      end
      assert_equal "1:10611:G", db["rs189107123"]
    end
  end

  def __test_benchmark_tch
    engine = "HDB"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_tcb
    engine = "BDB"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_kch
    engine = "kch"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_kcb
    engine = "kct"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_cdb
    engine = "CDB"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_leveldb
    engine = "LevelDB"
    run_bechmark(tsv_path, engine)
  end

  def __test_benchmark_lmdb
    engine = "LMDB"
    run_bechmark(tsv_path, engine)
  end

  def test_tsv_persist
    content =<<-EOF
#: :cast=:to_f
#Key Value1 Value2
k1 1.1 2.1
k2 1.2 2.2
    EOF
    TmpFile.with_file(content) do |tsv_file|
      tsv = TSV.open(tsv_file, :sep => " ", :persist => true, :type => :double, :merge => true)
      assert_equal [[1.1], [2.1]], tsv["k1"]
    end
  end
end
