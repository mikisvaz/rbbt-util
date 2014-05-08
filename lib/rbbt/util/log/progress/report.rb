module Log
  class ProgressBar
    def up_lines(depth)
      "\033[#{depth + 1}F\033[2K"
    end

    def down_lines(depth)
      "\n\033[#{depth + 2}E"
    end

    def print(io, str)
      LOG_MUTEX.synchronize do
        STDERR.print str
        Log.logfile.puts str unless Log.logfile.nil?
        Log::LAST.replace "progress"
      end
    end

    attr_accessor :history, :mean_max
    def thr
      count = @ticks - @last_count
      seconds = Time.now - @last_time
      thr = count / seconds
    end

    def thr_msg
      thr = self.thr
      if @history.nil?
        @history ||= []
      else
        @history << thr
        @history.shift if @history.length > 10
      end

      @mean_max ||= 0
      if @history.length > 3
        mean = @mean = Misc.mean(@history)
        @mean_max = mean if mean > @mean_max
      end

      str = "#{ Log.color :blue, thr.to_i.to_s } per sec."
      str << " #{ Log.color :yellow, mean.to_i.to_s } avg. #{Log.color :yellow, @mean_max.to_i.to_s} max." if @mean_max > 0
      str
    end

    def eta_msg
      percent = self.percent
      time = Time.now

      indicator = ""
      10.times{|i|
        if i < percent / 10 then
          indicator << Log.color(:yellow, ".")
        else
          indicator << " "
        end
      }

      indicator << " #{Log.color(:blue, percent.to_s << "%")}"

      used = time - @start
      if @mean_max and @mean_max > 0
        eta =  (@max - @ticks) / @mean
      else
        eta =  (@max - @ticks) / (@ticks/used)
      end

      used = [used/3600, used/60 % 60, used % 60].map{|t|  "%02i" % t }.join(':')
      eta = [eta/3600, eta/60 % 60, eta % 60].map{|t| "%02i" % t }.join(':')

      indicator << " #{Log.color :yellow, used} used #{Log.color :yellow, eta} left"

      indicator
    end

    def report_msg
      str = Log.color :magenta, desc
      return str << " " << Log.color(:yellow, "waiting") if @ticks == 0
      str << " " << thr_msg
      str << Log.color(:blue, " -- ") << eta_msg  if max
      str
    end

    def report(io = STDERR)
      if Log::LAST != "progress"
        length = Log::ProgressBar.cleanup_bars
        print(io, Log.color(:yellow, "--Progress\n"))
        bars = BARS
        bars.sort_by{|b| b.depth }.reverse.each do |bar|
          print(io, Log.color(:yellow ,bar.report_msg) << "\n")
        end
      end
      print(io, up_lines(@depth) << report_msg << down_lines(@depth)) 
      @last_time = Time.now
      @last_count = ticks
      @last_percent = percent if max
    end

    def done(io = STDERR)
      done_msg = Log.color(:magenta, desc) << " " << Log.color(:green, "done")
      ellapsed = (Time.now - @start).to_i
      ellapsed = [ellapsed/3600, ellapsed/60 % 60, ellapsed % 60].map{|t| "%02i" % t }.join(':')
      done_msg << " " << Log.color(:blue, (@ticks).to_s) << " in " << Log.color(:green, ellapsed)
      @last_count = 0
      @last_time = @start
      done_msg << " (" << thr_msg << ")"
      print(io, up_lines(@depth) << done_msg << down_lines(@depth)) 
    end
  end
end
