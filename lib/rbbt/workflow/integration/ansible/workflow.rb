require 'rbbt/workflow'

module Ansible
  module AnsibleWorkflow
    def self.extended(object)
      class << object
        attr_accessor :ans_tasks
      end

      object.helper :register do |task_info|
        desc = task.description if task
        name ||= desc || short_path
        task_info = {"name" => name}.merge(task_info)
        @ans_tasks ||= []
        @ans_tasks << task_info
        task
      end

      object.helper :ans do |name, info|
        register({ name => info})
      end

      object.helper :add do |name, info|
        @ans_tasks.last[name.to_s] = info
      end

      object.helper :shell do |cmd|
        register({"shell" => cmd.strip})
      end

      object.helper :sudo do |cmd|
        register({"shell" => cmd.strip, "become" => 'yes'})
      end

      object.helper :singularity do |scmd|
        img = config :singularity_img, :build, :test, :small, :default => '/data/img/singularity/rbbt/rbbt.simg'
        container = config :singularity_container, :build, :test, :small, :default => '/data/img/sandbox/mvazque2/'
        cmd = <<-EOF
singularity exec -C -H '#{container}' '#{img}' #{scmd}
        EOF
        register({"shell" => cmd.strip, "name" => short_path})
      end


      object.helper :produce_task do
        @ans_tasks
      end
    end

    def play(name = nil, &block)
      name = Misc.snake_case(@description) if name.nil?
      task name => :yaml do |*args|
        self.instance_exec *args, &block
        dependencies.inject([]){|acc,dep| acc += dep.load } + produce_task
      end
    end

  end
end

