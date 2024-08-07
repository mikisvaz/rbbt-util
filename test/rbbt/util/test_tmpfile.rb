require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestTmpFile < Test::Unit::TestCase

  def test_tmp_file
    assert(TmpFile.tmp_file("test") =~ /(tmpfiles|tmp)\/test\d+$/)
  end

  def test_do_tmp_file
    content = "Hello World!"
    TmpFile.with_file(content) do |file|
      assert_equal content, File.open(file).read
    end
  end

  def test_extension
    TmpFile.with_file(nil, true, :extension => 'txt') do |file|
      assert file =~ /\.txt$/
    end
  end

  def test_tmpdir
    TmpFile.with_file(nil, true, :tmpdir => 'TMPDIR') do |file|
      assert file =~ /TMPDIR/
    end
  end

end


