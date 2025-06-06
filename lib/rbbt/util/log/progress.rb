#require 'rbbt/util/log'
#require 'rbbt/util/log/progress/util'
#require 'rbbt/util/log/progress/report'
#module Log
#
#  def self.no_bar=(value)
#    @@no_bar = value
#  end
#
#  def self.no_bar
#    @@no_bar = false unless defined?(@@no_bar)
#    @@no_bar || ENV["RBBT_NO_PROGRESS"] == "true"
#  end
#
#  class ProgressBar
#
#    class << self
#      attr_accessor :default_file
#    end
#
#    attr_accessor :max, :ticks, :frequency, :depth, :desc, :file, :bytes, :process, :callback
#
#    def initialize(max = nil, options = {})
#      options = Misc.add_defaults options, :depth => 0, :num_reports => 100, :io => STDERR, :severity => Log.severity, :frequency => 2
#      depth, num_reports, desc, io, severity, file, bytes, frequency, process, callback = Misc.process_options options, :depth, :num_reports, :desc, :io, :severity, :file, :bytes, :frequency, :process, :callback
#
#      @max = max
#      @ticks = 0
#      @frequency = frequency
#      @last_time = nil
#      @last_count = nil
#      @last_percent = nil
#      @depth = depth
#      @desc = desc.nil? ? "" : desc.gsub(/\n/,' ')
#      @file = file
#      @bytes = bytes
#      @process = process
#      @callback = callback
#    end
#
#    def percent
#      return 0 if @ticks == 0
#      return 100 if @max == 0
#      (@ticks * 100) / @max
#    end
#
#    def file
#      @file || ProgressBar.default_file
#    end
#
#    def init
#      @ticks, @bytes = 0
#      @last_time = @last_count = @last_percent = nil
#      @history, @mean_max, @max_history = nil
#      @start = @last_time = Time.now
#      @last_count = 0
#      report
#    end
#
#    def tick(step = 1)
#      return if Log.no_bar
#      @ticks += step
#
#      time = Time.now
#      if @last_time.nil?
#        @last_time = time
#        @last_count = @ticks
#        @start = time
#        return
#      end
#
#      diff = time - @last_time
#      report and return if diff >= @frequency
#      return unless max and max > 0
#
#      percent = self.percent
#      if @last_percent.nil?
#        @last_percent = percent
#        return
#      end
#      report and return if percent > @last_percent and diff > 0.3
#    end
#
#    def pos(pos)
#      step = pos - (@ticks || 0)
#      tick(step)
#    end
#
#    def process(elem)
#      case res = @process.call(elem)
#      when FalseClass
#        nil
#      when TrueClass
#        tick
#      when Integer
#        pos(res)
#      when Float
#        pos(res * max)
#      end
#    end
#  end
#end
