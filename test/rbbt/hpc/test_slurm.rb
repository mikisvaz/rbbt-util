require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/hpc/slurm'
require 'rbbt/workflow'

require_relative 'hpc_test_workflows'
class TestSLURM < Test::Unit::TestCase
  def test_template
    job = TestWFA.job(:a2, "TEST")

    TmpFile.with_file do |batch_dir|

      template = HPC::SLURM.job_template(job, :batch_dir => batch_dir, :batch_modules => 'java', :time => '10min')
      assert_include template, "scout workflow task TestWFA a2"
      assert_include template, "00:10:00"
    end
  end

  def __test_run_job
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    job.clean

    jobid = HPC::SLURM.run_job(job, :workflows => "HTS", :batch_modules => 'java', :env_cmd => '_JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m"', :queue => :debug, :time => '01:00:00', :config_keys => "HTS_light", :task_cpus => '10', :tail => true, :clean_task => "HTS#mutect2")
    assert jobid.to_s =~ /^\d+$/
  end

end

