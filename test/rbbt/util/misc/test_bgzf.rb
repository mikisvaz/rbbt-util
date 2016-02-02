require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/misc/bgzf'

class TestBgzf < Test::Unit::TestCase
  def test_Bgzf
    content = "1234567890" * 1000000
    TmpFile.with_file(content) do |file|
      compressed = file + '.gz'
      `bgzip #{file} -c > #{compressed}`
      stream = Bgzf.setup File.open(compressed)
      stream.seek 500003
      assert_equal "4567", stream.read(4)
      assert_equal "89", stream.read(2)
    end
  end

  def test_tsv
    require 'rbbt/tsv'
    TmpFile.with_file(datafile_test(:identifiers).read) do |file|
      Misc.benchmark do
        tsv = TSV.open(Open.open(file))
      end
      compressed = file + '.bgz'

      `bgzip #{file} -c > #{compressed}`
      stream = Bgzf.setup File.open(compressed)
      Misc.benchmark do
        tsv = TSV.open(stream)
      end

      `gzip #{file}`
      stream = Open.open(file + '.gz')
      Misc.benchmark do
        tsv = TSV.open(stream)
      end
    end
  end

  def test_bgzip
    assert File.exists?(Bgzf.bgzip_cmd)
    assert 'bgzip', File.basename(Bgzf.bgzip_cmd)
  end
end

