require 'rbbt/util/tsv'
require 'rbbt/util/open'
require 'rbbt/util/log'

module RakeHelper
  def self.files(rakefile, task = :default, chdir = nil)
    status = nil
    files = nil
    TmpFile.with_file do |f|
      pid = Process.fork{
        require 'rake'
        FileUtils.chdir chdir if chdir

        Rake::FileTask.module_eval do
          class << self
            alias_method :old_define_task, :define_task
          end
          def self.define_task(file, *args, &block)
            @@files ||= []
            @@files << file
            old_define_task(file, *args, &block)
          end

          def self.files
            @@files
          end
        end

        load rakefile

        Open.write(f, Rake::FileTask.files * "\n")
        exit
      }


      pid, status = Process.waitpid2(pid)
      files = Open.read(f).split("\n")
    end
    raise "Error getting files from Rake: #{ rakefile } " unless status.success?
    files
  end

  def self.run(rakefile, task = :default, chdir = nil)
    pid = Process.fork{
      require 'rake'
      FileUtils.chdir chdir if chdir

      Rake::FileTask.module_eval do
        class << self
          alias_method :old_define_task, :define_task
        end
        def self.define_task(file, *args, &block)
          @@files ||= []
          @@files << file
          old_define_task(file, *args, &block)
        end

        def self.files
          @@files
        end
      end

      load rakefile

      task(:default) do |t|
        Rake::FileTask.files.each do |file| Rake::Task[file].invoke end
      end

      Rake::Task[task].invoke
      exit
    }
    pid, status = Process.waitpid2(pid)

    raise "Error in Rake: #{ rakefile } => #{ task }" unless status.success?
  end
end


