require 'rake'
module Rake
  class TaskNotFound < StandardError; end
  def self.run(rakefile, dir, task)
    old_pwd = FileUtils.pwd

    Rake::FileTask.module_eval do
      if not self.respond_to? :old_define_task
        class << self
          alias_method :old_define_task, :define_task
        end

        def self.define_task(file, *args, &block)
          @@files ||= []
          @@files << file
          old_define_task(file, *args, &block)
        end
      end

      def self.files
        @@files
      end
      
      def self.clear_files
        @@files = []
      end
    end

    Rake::Task.clear
    Rake::FileTask.clear_files

    if block_given?
      yield
    else
      load rakefile
    end

    raise TaskNotFound if Rake::Task[task].nil?

    t = nil
    pid = Process.fork{
      Misc.in_dir(dir) do
        Rake::Task[task].invoke

        Rake::Task.clear
        Rake::FileTask.clear_files
      end
    }
    Process.wait(pid)

  end
end
