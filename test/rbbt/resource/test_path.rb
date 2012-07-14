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

  def test_doc_file
    path = Path.setup "lib/rbbt/resource.rb"
    assert_equal File.join('doc', path), path.doc_file
    assert_equal Path.setup(File.join('doc', path)).find(:lib), path.find(:lib).doc_file

    assert_equal "lib/rbbt/resource.rb", path.doc_file.source_for_doc_file
    assert_equal path.find, path.doc_file.find(:lib).source_for_doc_file
    assert_equal path.find, path.doc_file.source_for_doc_file.find

    assert_equal "doc/lib/rbbt/resource.doc", path.doc_file.set_extension('doc')
    assert_equal "lib/rbbt/resource.rb", path.doc_file.set_extension('doc').source_for_doc_file

    assert_equal "doc/lib/rbbt/resource.doc", path.doc_file.set_extension('doc')
  end
end
