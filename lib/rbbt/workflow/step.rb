require 'rbbt/persist'
require 'rbbt/persist/tsv'
require 'rbbt/util/log'
require 'rbbt/workflow/accessor'

class Step
  attr_accessor :path, :task, :inputs, :dependencies
  attr_accessor :pid

  class Aborted < Exception; end

  def initialize(path, task = nil, inputs = nil, dependencies = nil)
    @path = path
    @task = task
    @dependencies = case
                    when dependencies.nil? 
                      []
                    when Array === dependencies
                      dependencies
                    else
                      [dependencies]
                    end
    @inputs = inputs || []
  end

  def exec
    @task.exec_in self, *@inputs
  end

  def join
    if @pid.nil?
      while not done? do
        Log.debug "Waiting: #{info[:step]}"
        sleep 5
      end
    else
      Process.waitpid @pid
      @pid = nil
    end
    self
  end

  def run
    Persist.persist "Job", @task.result_type, :file => @path, :check => rec_dependencies.collect{|dependency| dependency.path}.uniq do
      log task.name, "Starting task: #{ name }"
      set_info :dependencies, @dependencies.collect{|dep| [dep.task.name, dep.name]}
      @dependencies.each{|dependency| dependency.run}
      set_info :status, :start
      set_info :inputs, Misc.zip2hash(task.inputs, @inputs) unless task.inputs.nil?
      res = exec
      set_info :status, :done
      res
    end
  end

  def fork
    raise "Can not fork: Step is waiting for proces #{@pid} to finish" if not @pid.nil?
    @pid = Process.fork do
      begin
        trap(:INT) { raise Step::Aborted.new "INT signal recieved" }
        run
      rescue Exception
        log(:error, "#{$!.class}: #{$!.message}")
      end
    end
    set_info :pid, @pid
    self
  end

  def abort
    if @pid.nil?
      false
    else
      Process.kill("INT", @pid)
      true
    end
  end

  def load
    raise "Can not load: Step is waiting for proces #{@pid} to finish" if not done?
    Persist.persist "Job", @task.result_type, :file => @path, :check => rec_dependencies.collect{|dependency| dependency.path} do
      exec
    end
  end

  def clean
    if File.exists?(path) or File.exists?(info_file)
      begin
        FileUtils.rm info_file if File.exists? info_file
        FileUtils.rm path if File.exists? path
        FileUtils.rm path + '.lock' if File.exists? path + '.lock'
        FileUtils.rm_rf files_dir if File.exists? files_dir
      end
    end
    self
  end

  def rec_dependencies
    @dependencies.collect{|step| step.rec_dependencies}.flatten.concat  @dependencies
  end
  def step(name)
    rec_dependencies.select{|step| step.task.name.to_sym == name.to_sym}.first
  end
end
