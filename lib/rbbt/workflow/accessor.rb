require 'rbbt/util/open'
require 'yaml'

class Step

  def name
    @path.sub(/.*\/#{Regexp.quote task.name.to_s}\/(.*)/, '\1')
  end

  def clean_name
    name.sub(/(.*)_.*/, '\1')
  end

  # {{{ INFO

  def info_file
    @path + '.info'
  end

  def info
    return {} if not File.exists? info_file
    YAML.load(Open.open(info_file)) || {}
  end

  def set_info(key, value)
    Misc.lock(info_file) do
      i = info
      i[key] = value
      Open.write(info_file, i.to_yaml)
      value
    end
  end

  def status
    info[:status]
  end

  def status=(status)
    set_info(:status, status)
  end

  def messages
    info[:messages] || set_info(:messages, [])
  end

  def message(message)
    set_info(:messages, messages << message)
  end

  def log(status, message = nil)
    if message
      Log.low "#{ status }: #{ message }"
    else
      Log.low "#{ status }"
    end
    self.status = status
    message(message) unless message.nil?
  end

  def done?
    status = info[:status]
    status == :done or status == :error
  end

  def error?
    info[:status] == :error
  end

  # {{{ INFO

  def files_dir
    @path + '.files'
  end

  def files
    files = Dir.glob(File.join(files_dir, '**', '*')).reject{|path| File.directory? path}.collect do |path| 
      Misc.path_relative_to(files_dir, path) 
    end
    files
  end

  def file(name)
    Path.setup(File.join(files_dir, name.to_s))
  end

  def save_file(name, content)
    content = case
              when String === content
                content
              when Array === content
                content * "\n"
              when TSV === content
                content.to_s
              when Hash === content
                content.collect{|*p| p * "\t"} * "\n"
              else
                content.to_s
              end
    Open.write(file(name), content)
  end

  def load_file(name, type = nil, options = {})
    if type.nil? and name =~ /.*\.(\w+)$/
      extension = name.match(/.*\.(\w+)$/)[1]
      case extension
      when "tc"
        type = :tc
      when "tsv"
        type = :tsv
      when "list", "ary", "array"
        type = :array
      when "yaml"
        type = :yaml
      when "marshal"
        type = :marshal
      else
        type = :other
      end
    else
      type ||= :other
    end

    case type.to_sym
    when :tc
      Persist.open_tokyocabinet(file(name), false)
    when :tsv
      TSV.open Open.open(file(name)), options
    when :array
      Open.read(file(name)).split /\n|,\s*/
    when :yaml
      YAML.load(Open.open(file(name)))
    when :marshal
      Marshal.load(Open.open(file(name)))
    else
      Open.read(file(name))
    end
  end


end

module Workflow
  def task_info(name)
    task = tasks[name]
    description = task.description
    result_description = task.result_description
    result_type = task.result_type
    inputs = rec_inputs(name)
    input_types = rec_input_types(name)
    input_descriptions = rec_input_descriptions(name)
    input_defaults = rec_input_defaults(name)
    export = case
             when (synchronous_exports.include?(name.to_sym) or synchronous_exports.include?(name.to_s))
               :synchronous
             when (asynchronous_exports.include?(name.to_sym) or asynchronous_exports.include?(name.to_s))
               :asynchronous
             when (exec_exports.include?(name.to_sym) or exec_exports.include?(name.to_s))
               :exec
             else
               :none
             end

    
    dependencies = @task_dependencies[name].select{|dep| String === dep or Symbol === dep}
    { :id => File.join(self.to_s, name.to_s),
      :description => description,
      :export => export,
      :inputs => inputs,
      :input_types => input_types,
      :input_descriptions => input_descriptions,
      :input_defaults => input_defaults,
      :result_type => result_type,
      :result_description => result_description,
      :dependencies => dependencies
    }
  end

  def rec_dependencies(taskname)
    if task_dependencies.include? taskname
      deps = task_dependencies[taskname].select{|dep| String === dep or Symbol === dep}
      deps.concat deps.collect{|dep| rec_dependencies(dep)}.flatten
      deps.uniq
    else
      []
    end
  end

  def rec_inputs(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject([]){|acc, tn| acc.concat tasks[tn].inputs}
  end

  def rec_input_defaults(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn| acc.merge tasks[tn].input_defaults}
  end

  def rec_input_types(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn| acc.merge tasks[tn].input_types}
  end

  def rec_input_descriptions(taskname)
    [taskname].concat(rec_dependencies(taskname)).inject({}){|acc, tn| acc.merge tasks[tn].input_descriptions}
  end

  def real_dependencies(task, jobname, inputs, dependencies)
    real_dependencies = []
    dependencies.each do |dependency|
      real_dependencies << case
      when Step === dependency
        dependency
      when Symbol === dependency
        job dependency, jobname, inputs
      when Proc === dependency
        dependency.call jobname, inputs
      end
    end
    real_dependencies.flatten.compact
  end

  TAG = :hash
  def step_path(taskname, jobname, inputs, dependencies)
    raise "Jobname makes an invalid path: #{ jobname }" if jobname =~ /\.\./
    if inputs.any? or dependencies.any?
      tagged_jobname = case TAG
                       when :hash
                         jobname + '_' + Misc.digest((inputs + dependencies.collect{|dep| dep.name}).inspect)
                       else
                         jobname
                       end
    else
      tagged_jobname = jobname
    end

    workdir[taskname][tagged_jobname].find
  end


  def id_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
    Misc.path_relative_to workdir_find, path
  end

  def task_for(path)
    if workdir.respond_to? :find
      workdir_find = workdir.find 
    else
      workdir_find = workdir
    end
 
    Misc.path_relative_to(workdir_find, File.dirname(path)).sub(/([^\/]+)\/.*/,'\1')
  end
end
