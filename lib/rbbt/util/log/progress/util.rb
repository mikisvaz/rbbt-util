module Log
  class ProgressBar
    BAR_MUTEX = Mutex.new
    BARS = []
    REMOVE = []
    SILENCED = []

    def self.new_bar(max, options = {})
      cleanup_bars
      BAR_MUTEX.synchronize do
        #Log::LAST.replace "new_bar" if Log::LAST == "progress"
        options = Misc.add_defaults options, :depth => BARS.length
        BARS << (bar = ProgressBar.new(max, options))
        bar
      end
    end

    def self.cleanup_bars
      BAR_MUTEX.synchronize do
        REMOVE.each do |bar|
          index = BARS.index bar
          if index
            BARS.delete_at index
            BARS.each_with_index do |bar,i|
              bar.depth = i
            end
          end
          index = SILENCED.index bar
          if index
            SILENCED.delete_at index
            SILENCED.each_with_index do |bar,i|
              bar.depth = i
            end
          end
        end
        REMOVE.clear
        BARS.length
      end
    end

    def self.remove_bar(bar)
      bar.done if bar.respond_to? :done
      BAR_MUTEX.synchronize do
        REMOVE << bar
      end
    end

    def self.with_bar(max, options = {})
      bar = new_bar(max, options)
      begin
        yield bar
        keep = false
      rescue KeepBar
        keep = true
      ensure
        remove_bar(bar) if bar
      end
    end
  end
end

