require 'rbbt/workflow'
class RemoteWorkflow
  include Workflow

  attr_accessor :url, :name, :exec_exports, :synchronous_exports, :asynchronous_exports, :stream_exports

  def initialize(url, name)
    Log.debug{ "Loading remote workflow #{ name }: #{ url }" }
    @url, @name = url, name

    rest = url.include?('ssh://') ? false : true

    if rest
      self.extend RemoteWorkflow::REST
    else
      self.extend RemoteWorkflow::SSH
    end

    init_remote_tasks
  end

  def to_s
    name
  end

  def __job(task, name = nil, inputs = {})
    task_info = task_info(task)
    fixed_inputs = {}
    input_types = IndiferentHash.setup(task_info[:input_types])

    inputs.each do |k,v| 
      k = k.to_sym
      if TSV === v
        fixed_inputs[k] = v.to_s
      else
        next if input_types[k].nil?
        case input_types[k].to_sym
        when :tsv, :array, :file, :text
          fixed_inputs[k] = (String === v and Open.exists?(v)) ? Open.open(v) : v
        else
          fixed_inputs[k] = v
        end
      end
    end

    stream_input = @can_stream ? task_info(task)[:input_options].select{|k,o| o[:stream] }.collect{|k,o| k }.first : nil
    step = RemoteStep.new(url, task, name, fixed_inputs, task_info[:input_types], task_info[:result_type], task_info[:result_description], @exec_exports.include?(task), @stream_exports.include?(task), stream_input)
    step.workflow = self
    step
  end

  def load_id(id)
    task, name = id.split("/")
    step = RemoteStep.new url, task, nil
    step.name = name
    step.result_type = task_info(task)[:result_type]
    step.result_description = task_info(task)[:result_description]
    step
  end
end

require 'rbbt/workflow/remote_workflow/driver'
require 'rbbt/workflow/remote_workflow/remote_step'

