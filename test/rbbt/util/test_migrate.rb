require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/util/migrate'

class TestMigrate < Test::Unit::TestCase
  def test_source_locate
    assert_equal 'var/jobs/', Rbbt.migrate_source_paths(Rbbt.root['var/jobs'].find(:user)).last
    assert_include Rbbt.migrate_source_paths(Rbbt.root['var/jobs'].find(:user))[1], (File.join(ENV["HOME"], '.scout/var/jobs'))
  end

  def test_migrate
    Open.rm_rf Rbbt.tmp.test.migration_test.find(:user)
    test_file = Rbbt.tmp.test.migration_test.migration_test_file.find(:user)
    Open.write(test_file, "TEST")
    TmpFile.with_file do |tmpdir|
      Misc.in_dir tmpdir do
        Rbbt.migrate('tmp/test/migration_test/migration_test_file', :current)
      end
      assert_equal "TEST", Open.read(File.join(tmpdir, 'tmp/test/migration_test/migration_test_file'))
    end
  end

  def __test_migrate_turbo
    Log.with_severity 0 do
    TmpFile.with_file do |tmpdir|
      Misc.in_dir tmpdir do
        Rbbt.migrate('etc/config', :current, :source => 'mn1')
      end
      assert_equal "TEST", Open.read(File.join(tmpdir, 'etc/config'))
    end
    end
  end
end

