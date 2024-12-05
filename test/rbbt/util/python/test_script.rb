require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

require 'scout/tsv'
class TestPythonScript < Test::Unit::TestCase
  def test_script
    result = RbbtPython.script <<-EOF, value: 2
result = value * 3
    EOF
    assert_equal 6, result
  end

  def test_script_tsv

    tsv = TSV.setup({}, "Key~ValueA,ValueB#:type=:list")
    tsv["k1"] = ["a1", "b1"]
    tsv["k2"] = ["a2", "b2"]

    TmpFile.with_file(tsv.to_s) do |tsv_file|
      TmpFile.with_file do |target|
        result = RbbtPython.script <<-EOF, file: tsv_file, target: target
import rbbt
df = rbbt.tsv(file)
result = df.loc["k2", "ValueB"]
rbbt.save_tsv(target, df)
        EOF
        assert_equal "b2", result

        assert_equal "b2", TSV.open(target, type: :list)["k2"]["ValueB"]
      end

    end
  end

  def test_script_tsv_save

    tsv = TSV.setup({}, "Key~ValueA,ValueB#:type=:list")
    tsv["k1"] = ["a1", "b1"]
    tsv["k2"] = ["a2", "b2"]

    TmpFile.with_file do |target|
      result = RbbtPython.script <<-EOF, df: tsv, target: target
result = df.loc["k2", "ValueB"]
rbbt.save_tsv(target, df)
      EOF
      assert_equal "b2", result

      assert_equal "b2", TSV.open(target, type: :list)["k2"]["ValueB"]
    end
  end

  def test_script_exception
      assert_raises ConcurrentStreamProcessFailed do
        result = RbbtPython.script <<-EOF
afsdfasdf
        EOF
      end
  end
end

