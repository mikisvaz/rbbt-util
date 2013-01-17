require 'base64'
require 'simplews'

class WorkflowSOAP < SimpleWS
  attr_accessor :workflow
  def job(jobid)
    workdir = @workflow.workdir
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
 
    @workflow.load_step(File.join(workdir_find, jobid))
  end

  def initialize(workflow, *args)
    super(workflow.to_s, *args)
    @workflow = workflow
    @workflow.synchronous_exports.each do |name| synchronous name end
    @workflow.asynchronous_exports.each do |name| asynchronous name end

    desc "Job management: Check the status of a job"
    param_desc :jobid => "Job identifier", :return => "Status code. Special status codes are: 'done' and 'error'"
    serve :status, [:jobid], :jobid => :string, :return => :string do |jobid|
      (job(jobid).status || :queued).to_s
    end

    desc "Job management: Return an array with the messages issued by the job"
    param_desc :jobid => "Job identifier", :return => "Array with message strings"
    serve :messages, ['jobid'], :job => :string, :return => :array do |jobid|
      job(jobid).messages || []
    end

    desc "Job management: Return a YAML string containing arbitrary information set up by the job"
    param_desc :jobid => "Job identifier", :return => "Hash with arbitrary values in YAML format"
    serve :info, ['jobid'], :jobid => :string, :return => :string do |jobid|
      job(jobid).info.to_yaml
    end

    desc "Job management: Load job result as string "
    param_desc :jobid => "Job identifier", :return => "String containing the result of the job"
    serve :load_string, %w(jobid), :jobid => :string, :return => :string do |jobid|
      Open.read(job(jobid).path)
    end

    desc "Job management: Abort the job"
    param_desc :jobid => "Job identifier"
    serve :abort, %w(jobid), :jobid => :string, :return => false do |jobid|
      job(jobid).abort
    end

    desc "Job management: Check if the job is done. Could have finished successfully, with error, or have been aborted"
    param_desc :jobid => "Job identifier", :return => "True if the job has status 'done', 'error' or 'aborted'"
    serve :done, %w(jobid), :jobid => :string, :return => :boolean do |jobid|
      [:done, :error, :aborted].include?((job(jobid).status || :queued).to_sym)
    end

    desc "Job management: Check if the job has finished with error. The last message is the error message"
    param_desc :jobid => "Job identifier", :return => "True if the job has status 'error'"
    serve :error, %w(jobid), :jobid => :string, :return => :boolean do |jobid|
      job(jobid).status.to_sym == :error
    end

    desc "Job management: Check if the job has finished with error. The last message is the error message"
    param_desc :jobid => "Job identifier", :return => "True if the job has status 'error'"
    serve :clean, %w(jobid), :jobid => :string, :return => nil do |jobid|
      job(jobid).clean
      nil
    end


  end

  def synchronous(*tasknames)
    tasknames.each do |name|
      name = name.to_sym
      task = @workflow.tasks[name]
      desc @workflow.task_description[name] if @workflow.task_description.include? name

      rec_inputs = @workflow.rec_inputs name
      rec_input_descriptions= @workflow.rec_input_descriptions name
      rec_input_types= @workflow.rec_input_types name

      param_desc rec_input_descriptions
      serve name, rec_inputs, rec_input_types, &task
    end
  end

  def asynchronous(*tasknames)
    tasknames.each do |name|
      task = @workflow.tasks[name]
      desc @workflow.task_description[name] if @workflow.task_description.include? name

      rec_inputs = @workflow.rec_inputs name
      rec_input_descriptions= @workflow.rec_input_descriptions name
      rec_input_types= @workflow.rec_input_types name

      param_desc rec_input_descriptions.merge(:suggested_name => "Suggested Name", :return => "Job identifier")
      serve name, [:suggested_name] + rec_inputs, rec_input_types.merge(:suggested_name => :string, :return => :string) do |jobname, *inputs|
        inputs = Hash[*@workflow.rec_inputs(name).zip(inputs).flatten]
    
        step = @workflow.job(name, jobname, inputs)
        step.fork
        @workflow.id_for step.path
      end
    end
  end
end

