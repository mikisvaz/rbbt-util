module Misc
  def self.benchmark(repeats = 1, message = nil)
    require 'benchmark'
    res = nil
    begin
      measure = Benchmark.measure do
        repeats.times do
          res = yield
        end
      end
      if message
        puts "#{message }: #{ repeats } repeats"
      else
        puts "Benchmark for #{ repeats } repeats"
      end
      puts measure
    rescue Exception
      puts "Benchmark aborted"
      raise $!
    end
    res
  end

  def self.profile_html(options = {})
    require 'ruby-prof'
    RubyProf.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      result = RubyProf.stop
      printer = RubyProf::MultiPrinter.new(result)
      TmpFile.with_file do |dir|
        FileUtils.mkdir_p dir unless File.exists? dir
        printer.print(:path => dir, :profile => 'profile')
        CMD.cmd("firefox  -no-remote  '#{ dir }'")
      end
    end

    res
  end

  def self.profile_graph(options = {})
    require 'ruby-prof'
    RubyProf.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      result = RubyProf.stop
      #result.eliminate_methods!([/annotated_array_clean_/])
      printer = RubyProf::GraphPrinter.new(result)
      printer.print(STDOUT, options)
    end

    res
  end

  def self.profile(options = {})
    require 'ruby-prof'
    RubyProf.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      result = RubyProf.stop
      printer = RubyProf::FlatPrinter.new(result)
      printer.print(STDOUT, options)
    end

    res
  end

  def self.memprof
    require 'memprof'
    Memprof.start
    begin
      res = yield
    rescue Exception
      puts "Profiling aborted"
      raise $!
    ensure
      Memprof.stop
      print Memprof.stats
    end

    res
  end
end
