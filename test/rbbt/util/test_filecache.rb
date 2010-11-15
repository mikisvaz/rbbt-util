require File.expand_path(File.dirname(__FILE__) + '/../../test_helper')
require 'rbbt/util/filecache'
require 'test/unit'

class TestFileCache < Test::Unit::TestCase

  def test_path
    assert_equal(File.join(Rbbt.cachedir,'3','2','1','123.ext'), FileCache.path('123.ext'))
  end

  def test_add_read
    filename = '123.ext'
    content = 'test'

    FileCache.add(filename, content)
    assert_equal(content, File.open(FileCache.path(filename)).read)
    assert_equal(content, FileCache.get(filename).read)

    FileCache.del(filename)
  end

  def test_add_io
    filename = '123.ext'
    content =<<-EOF
test test test
test test test
    EOF

    FileCache.add(filename, StringIO.new(content))
    assert_equal(content, File.open(FileCache.path(filename)).read)

    FileCache.del(filename)
  end

end
