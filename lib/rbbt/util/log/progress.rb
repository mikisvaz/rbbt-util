require 'rbbt/util/log'
require 'rbbt/util/log/progress/util'
require 'rbbt/util/log/progress/report'
module Log
  class ProgressBar

    attr_accessor :max, :ticks, :frequency, :depth, :desc
    def initialize(max = nil, options = {})
      options = Misc.add_defaults options, :depth => 0, :num_reports => 100, :desc => "Progress", :io => STDERR, :severity => Log.severity
      depth, num_reports, desc, io, severity = Misc.process_options options, :depth, :num_reports, :desc, :io, :severity

      @max = max
      @ticks = 0
      @frequency = 2
      @last_time = nil
      @last_count = nil
      @last_percent = nil
      @depth = depth
      @desc = desc
    end

    def percent
      (@ticks * 100) / @max
    end

    def tick(step = nil)
      return if ENV["RBBT_NO_PROGRESS"] == "true"
      @ticks += 1

      begin
        time = Time.now
        if @last_time.nil?
          @last_time = time
          @last_count = @ticks
          @start = time
          return
        end

        diff = time - @last_time
        report and return if diff > @frequency
        return unless max

        percent = self.percent
        if @last_percent.nil?
          @last_percent = percent
          return
        end
        report and return if percent > @last_percent and diff > 0.3
      rescue Exception
        Log.warn "Exception during report: " << $!.message
        Log.exception $!
      end
    end
  end
end
