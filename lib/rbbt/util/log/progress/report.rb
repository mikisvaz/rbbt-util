module Log
  class ProgressBar
    def up_lines(depth)
      "\033[#{depth + 1}F\033[2K"
    end

    def down_lines(depth)
      "\n\033[#{depth + 2}E"
    end

    def print(io, str)
      return if ENV["RBBT_NO_PROGRESS"] == "true"
      LOG_MUTEX.synchronize do
        STDERR.print str
        Log.logfile.puts str unless Log.logfile.nil?
        Log::LAST.replace "progress"
      end
    end

    attr_accessor :history, :mean_max, :max_history
    def thr
      count = @ticks - @last_count
      if @last_time.nil?
        seconds = 0.001
      else
        seconds = Time.now - @last_time
      end
      thr = count / seconds
    end

    def thr_msg
      thr = self.thr
      if @history.nil?
        @history ||= [thr]
      else
        @history << thr
        max_history ||= case 
                      when @ticks > 20
                        count = @ticks - @last_count
                        if @max
                          times = @max / count
                          num = times / 20
                          num = 2 if num < 2
                        else
                          num = 10
                        end
                        count * num
                      else
                        20
                      end
        max_history = 100 if max_history > 100
        @history.shift if @history.length > max_history
      end

      @mean_max ||= 0
      if @history.length > 3
        mean = @mean = Misc.mean(@history)
        @mean_max = mean if mean > @mean_max
      end

      if mean.nil? or mean.to_i > 1
        str = "#{ Log.color :blue, thr.to_i.to_s } per sec."
        str << " #{ Log.color :yellow, mean.to_i.to_s } avg. #{Log.color :yellow, @mean_max.to_i.to_s} max." if @mean_max > 0
      else
        str = "#{ Log.color :blue, (1/thr).ceil.to_s } secs each"
        str << " #{ Log.color :yellow, (1/mean).ceil.to_s } avg. #{Log.color :yellow, (1/@mean_max).ceil.to_s} min." if @mean_max > 0
      end

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
      if @mean_max and @mean_max > 0 and @mean > 0
        eta =  (@max - @ticks) / @mean
      else
        eta =  (@max - @ticks) / (@ticks/used)
      end

      used = Misc.format_seconds(used) 
      eta = [eta/3600, eta/60 % 60, eta % 60].map{|t| "%02i" % t }.join(':')

      #indicator << " #{Log.color :yellow, used} used #{Log.color :yellow, eta} left - #{Log.color :yellow, ticks.to_s} of #{Log.color :yellow, @max.to_s} #{bytes ? 'bytes' : 'items'}"
      indicator << " #{Log.color :yellow, eta} => #{Log.color :yellow, used} - #{Log.color :yellow, ticks.to_s} of #{Log.color :yellow, @max.to_s} #{bytes ? 'bytes' : 'items'}"

      indicator
    end

    def report_msg
      str = Log.color :magenta, desc
      if @ticks == 0
        if @max
          return str << " " << Log.color(:yellow, "waiting on #{@max} #{bytes ? 'bytes' : 'items'}") 
        else
          return str << " " << Log.color(:yellow, "waiting - PID: #{Process.pid}") 
        end
      end
      str << " " << thr_msg
      if max
        str << Log.color(:blue, " -- ") << eta_msg
      else
        str << Log.color(:blue, " -- ") << ticks.to_s << " #{bytes ? 'bytes' : 'items'}"
      end
      str
    end

    def save
      info = {:start => @start, :last_time => @last_time, :last_count => @last_count, :last_percent => @last_percent, :desc => @desc, :ticks => @ticks, :max => @max, :mean => @mean}
      info.delete_if{|k,v| v.nil?}
      Open.write(@file, info.to_yaml)
    end

    def report(io = STDERR)
      if Log::LAST != "progress"
        length = Log::ProgressBar.cleanup_bars
        bars = BARS
        print(io, Log.color(:yellow, "...Progress\n"))
        bars.sort_by{|b| b.depth }.reverse.each do |bar|
          if SILENCED.include? bar
            print(io, Log.color(:yellow ,bar.report_msg) << "\n") 
          else
            print(io, "\n") 
          end
        end
      else
        bars = BARS
      end

      print(io, up_lines(bars.length) << Log.color(:yellow, "...Progress\n") << down_lines(bars.length)) 
      print(io, up_lines(@depth) << report_msg << down_lines(@depth)) 
      @last_time = Time.now
      @last_count = ticks
      @last_percent = percent if max and max > 0
      save if @file
    end

    def done(io = STDERR)
      done_msg = Log.color(:magenta, desc) << " " << Log.color(:green, "done")
      if @start
        ellapsed = (Time.now - @start).to_i
      else
        ellapsed = 0
      end
      ellapsed = [ellapsed/3600, ellapsed/60 % 60, ellapsed % 60].map{|t| "%02i" % t }.join(':')
      done_msg << " " << Log.color(:blue, (@ticks).to_s) << " in " << Log.color(:green, ellapsed)
      @last_count = 0
      @last_time = @start
      done_msg << " (" << thr_msg << ")"
      print(io, up_lines(@depth) << done_msg << down_lines(@depth)) 
      Open.rm @file if @file and Open.exists? @file
    end

    def error(io = STDERR)
      done_msg = Log.color(:magenta, desc) << " " << Log.color(:red, "error")
      if @start
        ellapsed = (Time.now - @start).to_i
      else
        ellapsed = 0
      end
      ellapsed = [ellapsed/3600, ellapsed/60 % 60, ellapsed % 60].map{|t| "%02i" % t }.join(':')
      done_msg << " " << Log.color(:blue, (@ticks).to_s) << " in " << Log.color(:green, ellapsed)
      @last_count = 0
      @last_time = @start
      done_msg << " (" << thr_msg << ")"
      print(io, up_lines(@depth) << done_msg << down_lines(@depth)) 
      Open.rm @file if @file and Open.exists? @file
    end
  end
end
