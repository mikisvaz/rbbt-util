require File.expand_path(__FILE__).sub(%r(/test/.*), '/test/test_helper.rb')
require File.expand_path(__FILE__).sub(%r(.*/test/), '').sub(/test_(.*)\.rb/,'\1')

require 'rbbt/workflow'

class TestPBS < Test::Unit::TestCase
  def workflow
    @workflow ||= Module.new do
      extend Workflow

      def self.to_s
        "TestWorkflow"
      end

      input :name, :string
      task :hello => :string do |name|
        "hello #{name}"
      end
    end
  end

  def test_template
    job = workflow.job(:hello, "TEST", :name => "world")

    TmpFile.with_file do |batch_dir|

      template = HPC::PBS.job_template(job, :batch_dir => batch_dir, :lua_modules => 'java')
      assert_include template, "rbbt workflow task TestWorkflow hello"

    end
  end

  def __test_run_job
    job = Sample.job(:mutect2, "small", :reference => "hg38")

    job.clean

    jobid = HPC::SLURM.run_job(job, :workflows => "HTS", :batch_modules => 'java', :env_cmd => '_JAVA_OPTIONS="-Xms1g -Xmx${MAX_MEMORY}m"', :queue => :debug, :time => '01:00:00', :config_keys => "HTS_light", :task_cpus => '10', :tail => true, :clean_task => "HTS#mutect2")
    assert jobid.to_s =~ /^\d+$/
  end

end

