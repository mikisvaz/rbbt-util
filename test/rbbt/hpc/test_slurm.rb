require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/hpc/slurm'
require 'rbbt/workflow'

class TestSLURM < Test::Unit::TestCase
  def setup
    Workflow.require_workflow "Sample"
    Workflow.require_workflow "HTS"
  end

  def __test_template
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    TmpFile.with_file do |batch_dir|

      template = HPC::SLURM.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java')
      ppp template

    end
  end

  def __test_run_job
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    job.clean

    jobid = HPC::SLURM.run_job(job, :workflows => "HTS", :batch_modules => 'java', :env_cmd => '_JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m"', :queue => :debug, :time => '01:00:00', :config_keys => "HTS_light", :task_cpus => '10', :tail => true, :clean_task => "HTS#mutect2")
    assert jobid.to_s =~ /^\d+$/
  end

end

