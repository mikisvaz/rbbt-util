require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/hpc/batch'
require 'rbbt/workflow'

require_relative 'hpc_test_workflows'

class TestBatch < Test::Unit::TestCase

  def test_batch_options
    job = TestWFB.job(:b1, "TEST")

    TmpFile.with_file do |batch_dir|

      options = HPC::BATCH.batch_options(job, :batch_dir => batch_dir, :batch_modules => 'java')
      assert_equal options[:step_path], job.path
    end
  end

  def test_template
    job = TestWFB.job(:b1, "TEST")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :lua_modules => 'java')
      assert_include template, 'module load java'
    end
  end

  def test_template_singularity
    job = TestWFB.job(:b1, "TEST")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :lua_modules => 'java', :singularity => true)
      assert_include template, "singularity exec"
    end
  end

  def test_template_contain
    job = TestWFB.job(:b1, "TEST")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :lua_modules => 'java', :contain_and_sync => true, :wipe_container => 'force')
      assert_include template, "batch_erase_contain_dir"

    end
  end
  
  def test_template_singularity_contain
    job = TestWFB.job(:b1, "TEST")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :lua_modules => 'java', :contain_and_sync => true, :wipe_container => 'force', :singularity => true)
      assert_include template, "--workdir_all"
      assert_include template, "batch_erase_contain_dir"
      assert_include template, "singularity exec"

    end
  end
  
end

