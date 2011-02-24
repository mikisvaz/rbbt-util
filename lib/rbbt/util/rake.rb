require 'rake'
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
          if not self.respond_to? :old_define_task
            class << self
              alias_method :old_define_task, :define_task 
            end 
          end

          def self.define_task(file, *args, &block)
            @@files ||= []
            if Hash === file
              @@files << file.keys.first.to_s
            else
              @@files << file.to_s
            end
            old_define_task(file, *args, &block)
          end

          def self.files
            if defined? @@files
              @@files
            else
              @@file = []
            end
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
    require 'rake'
    old_pwd = FileUtils.pwd
    FileUtils.chdir chdir if chdir

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

    task(:default) do |t|
      Rake::FileTask.files.each do |file| Rake::Task[file].invoke end
    end

    Rake::Task[task].invoke

    Rake::Task.clear
    Rake::FileTask.clear_files

    FileUtils.chdir old_pwd
  end

  module WorkFlow
    attr_accessor :default_persistence, :stage_options, :run_options

    def stage_options(stage = :next)
      @stage_options ||= {}
      @stage_options[stage] ||= []
    end

    def run_options
      @run_options ||= {}
    end

    def stage(stage_name, options = nil)
      dependencies, options = case
                              when ((String === options or Symbol === options) and %w(string marshal yaml tsv tsv_string).include? options.to_s)
                                [nil, {:persistence_type => options}]
                              when Hash === options
                                [nil, options]
                              else
                                [options, {}]
                              end

      @stage_options[stage_name] = @stage_options[:next]

      options = Misc.add_defaults options, :persistence_type => default_persistence || :string
      persistence_type = Misc.process_options options, :persistence_type
      dependencies = Misc.process_options options, :dependencies if options.include? :dependencies

      re = Regexp.new(/(?:^|\/)#{Regexp.quote stage_name.to_s}\/.*$/)

      @last_step             = nil unless defined? @last_step
      @last_persistence_type = nil unless defined? @last_persistence_type

      if dependencies.nil? && ! @last_step.nil?
        dependencies = @last_step
      end
      @last_step = stage_name

      # Generate the Hash definition
      rule_def = case 
                 when dependencies.nil?
                   re
                 when String === dependencies 
                   {re => dependencies}
                 when Symbol === dependencies
                   {re => lambda{|filename| filename.sub(stage_name.to_s, dependencies.to_s) }}
                 when Array === dependencies
                   {re => lambda{|filename| dependencies.collect{|dep| filename.sub(stage_name.to_s, dep.to_s) } }}
                 when Proc === dependencies
                   {re => dependencies}
                 end

      @last_step = stage_name
      last_persistence_type, @last_persistence_type = @last_persistence_type, persistence_type

      rule rule_def do |t|
        Persistence.persist(t, "", persistence_type, :persistence_file => t.name) do |t, options|
          data = case
                 when (t.prerequisites.nil? or (Array === t.prerequisites and t.prerequisites.empty?))
                   nil
                 else
                   Persistence.persist(t.prerequisites.first, "", last_persistence_type, :persistence_file => t.prerequisites.first) do
                     raise "Error, this file should be produced already"
                   end
                 end
          options = @run_options.values_at *stage_options
          step stage_name, "Executing step [#{ stage_name }]"
          yield t, *options
        end
      end
    end
  end
end


