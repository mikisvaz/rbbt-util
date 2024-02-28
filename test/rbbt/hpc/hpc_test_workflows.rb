
require 'rbbt/workflow'
module TestWFA
  extend Workflow

  task :a1 => :string do self.task_name.to_s end

  dep :a1
  task :a2 => :string do self.task_name.to_s end

  dep :a2
  task :a3 => :string do self.task_name.to_s end
end

module TestWFB
  extend Workflow

  dep TestWFA, :a2
  task :b1 => :string do self.task_name.to_s end

  dep :b1
  task :b2 => :string do self.task_name.to_s end
end

module TestWFC
  extend Workflow

  dep TestWFA, :a1
  dep_task :c1, TestWFB, :b2

  task :c2 => :string do self.task_name.to_s end

  dep :c1
  dep :c2
  task :c3 => :string do self.task_name.to_s end
  
  dep_task :c4, TestWFC, :c3
end

module TestWFD
  extend Workflow

  dep TestWFC, :c3, :jobname => "First c3"
  dep TestWFC, :c3, :jobname => "Second c3"
  task :d1 => :string do self.task_name.to_s end
end

module TestDeepWF
  extend Workflow

  dep_task :deep1, TestWFD, :d1

  input :size, :integer, "Number of dependencies", 100
  dep :deep1 do |jobname,options|
    options[:size].to_i.times.collect{|i|
      {:jobname => "step-#{i}"}
    }
  end
  task :suite => :array do
  end
end

