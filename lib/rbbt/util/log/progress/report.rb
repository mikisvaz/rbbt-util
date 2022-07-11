module Log
  class ProgressBar
    def print(io, str)
      return if Log.no_bar
      LOG_MUTEX.synchronize do
        STDERR.print str
        Log.logfile.puts str unless Log.logfile.nil?
        Log::LAST.replace "progress"
      end
    end

    attr_accessor :history, :mean_max, :max_history
    def thr_msg
      if @history.nil?
        @history ||= [[0, @start], [@ticks, Time.now] ]
      elsif @last_ticks != @ticks
        @history << [@ticks, Time.now]
        max_history ||= begin
                          max_history = case 
                                        when @ticks > 20
                                          count = @ticks - @last_count
                                          count = 1 if count == 0
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
                          max_history = 30 if max_history > 30
                          max_history
                        end
        @history.shift if @history.length > max_history
      end

      @last_ticks = @ticks

      @mean_max ||= 0
      if @history.length > 3

        sticks, stime = @history.first
        ssticks, sstime = @history[-3]
        lticks, ltime = @history.last


        mean = @mean = (lticks - sticks).to_f / (ltime - stime)
        short_mean = (lticks - ssticks).to_f / (ltime - sstime)

        @mean_max = mean if mean > @mean_max
      end

      if short_mean
        thr = short_mean
      else
        thr = begin
                (@ticks || 1) / (Time.now - @start) 
              rescue
                1
              end
      end

      thr = 0.0000001 if thr == 0
      
      if mean.nil? or mean.to_i > 2
        str = "#{ Log.color :blue, thr.to_i.to_s } per sec."
        #str << " #{ Log.color :yellow, mean.to_i.to_s } avg. #{Log.color :yellow, @mean_max.to_i.to_s} max." if @mean_max > 0
      else
        if 1.0/thr < 1
          str = "#{ Log.color :blue, (1.0/thr).round(2).to_s } secs each"
        elsif 1.0/thr < 2
          str = "#{ Log.color :blue, (1.0/thr).round(1).to_s } secs each"
        else
          str = "#{ Log.color :blue, (1/thr).ceil.to_s } secs each"
        end
        #str << " #{ Log.color :yellow, (1/mean).ceil.to_s } avg. #{Log.color :yellow, (1/@mean_max).ceil.to_s} min." if @mean_max > 0
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
      str = Log.color(:magenta, "·")
      if @ticks == 0
        if @max
          return str << " " << Log.color(:magenta, "waiting on #{@max} #{bytes ? 'bytes' : 'items'}") <<  Log.color(:magenta, " · " << desc)
        else
          return str << " " << Log.color(:magenta, "waiting - PID: #{Process.pid}") <<  Log.color(:magenta, " · " << desc)
        end
      end
      str << " " << thr_msg
      if max
        str << Log.color(:blue, " -- ") << eta_msg
      else
        str << Log.color(:blue, " -- ") << ticks.to_s << " #{bytes ? 'bytes' : 'items'}"
      end
      str <<  Log.color(:magenta, " · " << desc)
      str
    end

    def load(info)
      info.each do |key, value| 
        case key.to_sym
        when :start 
          @start = value
        when :last_time 
          @last_time = value
        when :last_count 
          @last_count = value
        when :last_percent 
          @last_percent = value
        when :desc 
          @desc = value
        when :ticks 
          @ticks = value
        when :max 
          @max = value
        when :mean 
          @mean = value
        end
      end
    end

    def save
      info = {:start => @start, :last_time => @last_time, :last_count => @last_count, :last_percent => @last_percent, :desc => @desc, :ticks => @ticks, :max => @max, :mean => @mean}
      info.delete_if{|k,v| v.nil?}
      Open.write(file, info.to_yaml)
    end

    def report(io = STDERR)
      if Log::LAST != "progress"
        bars = BARS
        if Log::LAST == "new_bar"
          Log::LAST.replace "progress"
          bar = bars.sort_by{|b| b.depth }.first
          print(io, Log.color(:magenta ,bar.report_msg) << "\n") 
        else
          length = Log::ProgressBar.cleanup_bars
          print(io, Log.color(:magenta, "···Progress\n"))
          bars.sort_by{|b| b.depth }.reverse.each do |bar|
            if SILENCED.include? bar
              print(io, Log.color(:magenta, "·\n")) 
            else
              print(io, Log.color(:magenta ,bar.report_msg) << "\n") 
            end
          end
        end
      else
        bars = BARS
      end
      bars << self unless BARS.include? self

      print(io, Log.up_lines(bars.length) << Log.color(:magenta, "···Progress\n") << Log.down_lines(bars.length+1)) if Log::ProgressBar.offset == 0
      print(io, Log.up_lines(@depth) << report_msg << "\n" << Log.down_lines(@depth - 1)) 
      @last_time = Time.now
      @last_count = ticks
      @last_percent = percent if max and max > 0
      Log::LAST.replace "progress"
      save if file
    end

    def done(io = STDERR)
      done_msg = Log.color(:magenta, "· ") << Log.color(:green, "done")
      if @start
        ellapsed = (Time.now - @start).to_i
      else
        ellapsed = 0
      end
      ellapsed = [ellapsed/3600, ellapsed/60 % 60, ellapsed % 60].map{|t| "%02i" % t }.join(':')
      done_msg << " " << Log.color(:blue, (@ticks).to_s) << " #{bytes ? 'bytes' : 'items'} in " << Log.color(:green, ellapsed)
      @last_count = 0
      @last_time = @start
      done_msg << " - " << thr_msg 
      done_msg << Log.color(:magenta, " · " << desc)
      print(io, Log.up_lines(@depth) << done_msg << Log.down_lines(@depth)) 

      Open.rm file if file and Open.exists?(file)

      @callback.call self if @callback
    end

    def error(io = STDERR)
      done_msg = Log.color(:magenta, "· ") << Log.color(:red, "error")
      if @start
        ellapsed = (Time.now - @start).to_i
      else
        ellapsed = 0
      end
      ellapsed = [ellapsed/3600, ellapsed/60 % 60, ellapsed % 60].map{|t| "%02i" % t }.join(':')
      done_msg << " " << Log.color(:blue, (@ticks).to_s) << " in " << Log.color(:green, ellapsed)
      @last_count = 0
      @last_time = @start
      done_msg << " - " << thr_msg
      done_msg << Log.color(:magenta, " · " << desc)      
      print(io, Log.up_lines(@depth) << done_msg << Log.down_lines(@depth)) 

      Open.rm file if file and Open.exists?(file)

      begin
        @callback.call self
      rescue
        Log.debug "Callback failed for filed progress bar: #{$!.message}"
      end if @callback
    end
  end
end
