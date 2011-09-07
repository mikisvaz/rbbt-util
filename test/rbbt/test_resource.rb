require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt/resource'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestResource
  extend Resource

  claim tmp.test.google, :url, "http://google.com"
  claim tmp.test.string, :string, "TEST"
  claim tmp.test.proc, :proc do
    "PROC TEST"
  end

  claim tmp.test.rakefiles.foo , :string , <<-EOF
file 'foo' do |t|
  Open.write(t.name, "TEST")
end
  EOF

  claim tmp.test.work.footest, :rake, tmp.test.rakefiles.foo
end

class TestTSV < Test::Unit::TestCase

  def test_rake
    TestResource.tmp.test.work.footest.foo.read == "TEST"
    assert TestResource.tmp.test.work.footest.foo.read == "TEST"
  end

  def test_proc
    assert TestResource.tmp.test.proc.read == "PROC TEST"
  end

  def test_string
    assert TestResource.tmp.test.string.read == "TEST"
  end

  def test_url
    assert TestResource[].tmp.test.google.read =~ /google/
  end


  def test_resolve
    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Path.setup('etc/foo').find(:user)
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Path.setup('etc/foo', 'phgx').find(:user)

    assert_equal File.join('/', 'etc/rbbt/foo'), Path.setup('etc/foo').find(:global)
    assert_equal File.join('/', 'etc/phgx/foo'), Path.setup('etc/foo', 'phgx').find(:global)

    assert_equal File.join('/usr/local', 'etc/rbbt/foo'), Path.setup('etc/foo').find(:local)
    assert_equal File.join('/usr/local', 'etc/phgx/foo'), Path.setup('etc/foo', 'phgx').find(:local)

    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../', 'etc/foo')), Path.setup('etc/foo').find(:lib)
    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../', 'etc/foo')), Path.setup('etc/foo').find(:lib)

    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Path.setup('etc/foo').find()
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Path.setup('etc/foo', 'phgx').find()
  end

  def test_libdir
    assert File.exists? TestResource[].share.lib.R["util.R"].find :lib
    assert File.exists? TestResource[].share.lib.R["util.R"].find 
  end

end
