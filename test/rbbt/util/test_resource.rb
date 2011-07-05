require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/resource'


module Rbbt
  extend Resource

  tmp.test_string.define_as_string "Test String"
  tmp.url.define_as_url "http://www.ruby-lang.org/es/"
  tmp.Rakefile.define_as_string <<-EOF
file 'foo' do |t|
   Open.write(t.name, "Test String")
end
  EOF

end

Open.cachedir = Rbbt.tmp.cache.find :user

module Phgx
  extend Resource
end


class TestResource < Test::Unit::TestCase
  def test_methods
    assert Resource.methods.collect{|m| m.to_s}.include?("resources") 
    assert ! Resource.methods.collect{|m| m.to_s}.include?("pkgdir")
    assert ! Phgx.methods.collect{|m| m.to_s}.include?("resources")
    assert Phgx.methods.collect{|m| m.to_s}.include?("pkgdir")

  end
  def test_resolve
    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Resource.resolve('etc/foo', '', :user)
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Resource.resolve('etc/foo', 'phgx', :user)

    assert_equal File.join('/', 'etc/foo'), Resource.resolve('etc/foo', '', :global)
    assert_equal File.join('/', 'etc/phgx/foo'), Resource.resolve('etc/foo', 'phgx', :global)

    assert_equal File.join('/usr/local', 'etc/foo'), Resource.resolve('etc/foo', '', :local)
    assert_equal File.join('/usr/local', 'etc/phgx/foo'), Resource.resolve('etc/foo', 'phgx', :local)

    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../../', 'etc/foo')), Resource.resolve('etc/foo', '', :lib)
    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../../', 'etc/foo')), Resource.resolve('etc/foo', 'phgx', :lib)

    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Resource.resolve('etc/foo', '')
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Resource.resolve('etc/foo', 'phgx')
  end

  def test_base
    assert_equal Rbbt, Rbbt.base
  end

  def test_path
    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Rbbt.etc.foo.find
    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Rbbt.etc.foo.find(:user)
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Phgx.etc.foo.find
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Phgx.etc.foo.find(:user)
  end

  def test_libdir
    assert File.exists? Rbbt.share.lib.R["util.R"].find :lib
    assert File.exists? Rbbt.share.lib.R["util.R"].find 
  end

  def test_define
    begin
      assert_equal "Test String", Rbbt.tmp.test_string.read
      assert_equal "Test String", Rbbt.tmp.work.foo.read
    rescue
    ensure
      FileUtils.rm Rbbt.tmp.test_string.find if File.exists? Rbbt.tmp.test_string.find
      FileUtils.rm Rbbt.tmp.url.find if File.exists? Rbbt.tmp.url.find
    end
  end
end

