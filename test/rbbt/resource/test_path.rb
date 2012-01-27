require File.expand_path(File.dirname(__FILE__) + '../../../test_helper')
require 'rbbt/resource/path'
require 'rbbt/util/tmpfile'
require 'test/unit'

class TestTSV < Test::Unit::TestCase

  def test_find
    name = 'test_name_for_unexistent_file'
    path = Path.setup "tmp/#{ name }"

    TmpFile.with_file do |dir|
      assert File.join(ENV['HOME'], path), path.find(nil, nil, {:root => File.join(dir, '{TOPLEVEL}/{SUBPATH}'), :default => :user, :user => File.join(ENV['HOME'], "{TOPLEVEL}", "{SUBPATH}")})
      FileUtils.mkdir_p File.dirname(File.join(dir, path))
      FileUtils.touch File.join(dir, path)
      assert File.join(dir, "tmp/test"), path.find(nil, nil, {:root => File.join(dir, '{TOPLEVEL}/{SUBPATH}'), :default => :user, :user => File.join(ENV['HOME'], "{TOPLEVEL}", "{SUBPATH}")})
    end
  end

  def test_prev
    path = Path.setup "/tmp"
    assert_equal "/tmp/bar/foo", path.foo("bar")
  end
end
