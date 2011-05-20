require 'simplews'
require 'rbbt/util/workflow'
require 'base64'

class WorkFlowWS < SimpleWS

  def self.klass=(klass)
    @klass = klass
  end

  def self.klass
    @klass || self
  end

  def task(name)
    self.class.klass.tasks[name]
  end

  def export(name)
    task = self.class.klass.tasks[name]

    options, optional_options = task.option_summary

    desc task.description
    options.each do |option|
      param_desc option[:name] => option[:description] if option[:description]
    end
    param_desc :return => "Job Identifier"
    option_names = [:name] + options.collect{|option| option[:name]}
    option_types = Hash[*option_names.zip([ :string] + options.collect{|option| option[:type] || :string}).flatten]
    serve name, option_names, option_types do |*args|
      task(name).job(*args).fork.id
    end
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  serve :abort, %w(task id), {:return => false, :task => :string, :id => :string} do |task, id|
    task(task).load(id).abort
    nil
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "Info hash in yaml"
  serve :info, %w(task id), {:task => :string, :id => :string} do |task, id|
    Open.read(task(task).load(id).info_file)
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "Step"
  serve :step, %w(task id), {:task => :string, :id => :string} do |task, id|
    task(task).load(id).step.to_s
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "True if job is done (error or not)"
  serve :done, %w(task id), {:return => :boolean, :task => :string, :id => :string} do |task, id|
    task(task).load(id).done?
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "True if job finished with error. Error message is the last of the messages (see 'messages' method)."
  serve :error, %w(task id), {:return => :boolean, :task => :string, :id => :string} do |task, id|
    task(task).load(id).error?
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "Messages"
  serve :messages, %w(task id), {:return => :array, :task => :string, :id => :string} do |task, id|
    task(task).load(id).messages
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "Job result in Base64"
  serve :load, %w(task id), {:return => :binary, :task => :string, :id => :string} do |task, id|
    Base64.encode64(task(task).load(id).read)
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :return => "File names"
  serve :files, %w(task id), {:return => :array, :task => :string, :id => :string} do |task, id|
    task(task).load(id).files
  end

  param_desc :task => "Task name"
  param_desc :id   => "Job id"
  param_desc :file   => "File name"
  param_desc :return => "File contents in Base64"
  serve :file, %w(task id file), {:return => :array, :task => :string, :id => :string, :file => :string} do |task, id|
    Base64.encode64(task(task).load(id).files(file).read)
  end
end


if __FILE__ == $0

  require 'rbbt/sources/organism/sequence'
  class SequenceWF < WorkFlowWS
    self.klass = Organism
  end
  
  wf = SequenceWF.new
  wf.export :genomic_mutations_to_genes
  Open.write('/tmp/foo.wsdl', wf.wsdl)
  wf.start
end



