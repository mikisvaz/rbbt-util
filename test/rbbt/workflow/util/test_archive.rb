require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt-util'
require 'rbbt/workflow'
require 'rbbt/workflow/util/archive'

module ArchiveTestWF
  extend Workflow
  task :test_archive => :string do
    Open.write(file(:file1), "Test file")
    "TEST"
  end
end

class TestClass < Test::Unit::TestCase
  def test_migrate
    job = ArchiveTestWF.job(:test_archive)
    job.run

    Log.with_severity 0 do
      TmpFile.with_file do |tmpdir|
        Misc.in_dir tmpdir do
          Step.migrate(job.path, :current, :delete => false, :print => false)
        end
        assert_equal "TEST", Open.read(File.join(tmpdir, 'var/jobs/ArchiveTestWF/test_archive/Default'))
      end
    end


  end
end

