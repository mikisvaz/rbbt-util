#module Log
#  class ProgressBar
#    BAR_MUTEX = Mutex.new
#    BARS = []
#    REMOVE = []
#    SILENCED = []
#
#    def self.add_offset(value = 1)
#      value = 1 if TrueClass === value
#      @@offset = offset + value.to_i
#      @@offset = 0 if @@offset < 0
#      @@offset
#    end
#
#    def self.remove_offset(value = 1)
#      value = 1 if TrueClass === value
#      @@offset = offset - value.to_i
#      @@offset = 0 if @@offset < 0
#      @@offset
#    end
#
#
#    def self.offset
#      @@offset ||= 0
#      @@offset = 0 if @@offset < 0
#      @@offset
#    end
#
#    def self.new_bar(max, options = {})
#      cleanup_bars
#      BAR_MUTEX.synchronize do
#        Log::LAST.replace "new_bar" if Log::LAST == "progress"
#        options = Misc.add_defaults options, :depth => BARS.length + Log::ProgressBar.offset
#        BARS << (bar = ProgressBar.new(max, options))
#        bar
#      end
#    end
#
#    def self.cleanup_bars
#      BAR_MUTEX.synchronize do
#        REMOVE.each do |bar|
#          index = BARS.index bar
#          if index
#            BARS.delete_at index
#            BARS.each_with_index do |bar,i|
#              bar.depth = i
#            end
#          end
#          index = SILENCED.index bar
#          if index
#            SILENCED.delete_at index
#            SILENCED.each_with_index do |bar,i|
#              bar.depth = i
#            end
#          end
#        end
#        REMOVE.clear
#        BARS.length
#      end
#    end
#
#    def self.remove_bar(bar, error = false)
#      BAR_MUTEX.synchronize do
#        return if REMOVE.include? bar
#      end
#      if error
#        bar.error if bar.respond_to? :error
#      else
#        bar.done if bar.respond_to? :done
#      end
#      BAR_MUTEX.synchronize do
#        REMOVE << bar
#      end
#      Log::LAST.replace "remove_bar" if Log::LAST == "progress"
#    end
#
#    def remove(error = false)
#      Log::ProgressBar.remove_bar self, error
#    end
#
#    def self.with_bar(max, options = {})
#      bar = new_bar(max, options)
#      res = nil
#      begin
#        error = false
#        keep = false
#        yield bar
#      rescue KeepBar
#        keep = true
#      rescue
#        error = true
#        raise $!
#      ensure
#        remove_bar(bar, error) if bar
#      end
#    end
#  end
#end
#
