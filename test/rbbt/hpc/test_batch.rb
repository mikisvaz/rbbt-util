require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/hpc/batch'
require 'rbbt/workflow'

Workflow.require_workflow "Sample"
Workflow.require_workflow "HTS"
class TestSLURM < Test::Unit::TestCase

  def _test_batch_options
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      options = HPC::BATCH.batch_options(job, :batch_dir => batch_dir, :batch_modules => 'java')

      iii options
    end
  end

  def _test_template
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java')
      ppp template

    end
  end

  def _test_template_singularity
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java', :singularity => true)
      ppp template

    end
  end

  def _test_template_contain
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java', :contain_and_sync => true, :wipe_container => 'force')
      ppp template

    end
  end
  
  def test_template_singularity_contain
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      template = HPC::BATCH.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java', :contain_and_sync => true, :wipe_container => 'force', :singularity => true)
      ppp template

    end
  end
  
end

