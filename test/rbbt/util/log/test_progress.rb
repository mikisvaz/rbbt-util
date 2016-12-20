require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/log/progress'

class TestProgress < Test::Unit::TestCase
  def test_bar
    t1 = Thread.new do
      Log::ProgressBar.with_bar(20, :desc => "Bar 1") do |bar|
        20.times do
          bar.tick
          sleep 0.3
        end
        Log.debug "Done progress"
        assert_equal 100, bar.percent
      end
    end

    t2 = Thread.new do
      Log::ProgressBar.with_bar(20, :desc => "Bar 2") do |bar|
        20.times do
          bar.tick
          sleep 0.2
        end
        Log.debug "Done progress"
        assert_equal 100, bar.percent
      end
    end
    t1.join
    t2.join
  end

  def test_bar_no_size
    t1 = Thread.new do
      Log::ProgressBar.with_bar(nil, :desc => "Bar 1") do |bar|
        20.times do
          bar.tick
          sleep 0.3
        end
        #Log.debug "Done progress"
        assert bar.history.length > 0
      end
    end

    t2 = Thread.new do
      Log::ProgressBar.with_bar(nil, :desc => "Bar 2") do |bar|
        20.times do
          bar.tick
          sleep 0.2
        end
        #Log.debug "Done progress"
        assert bar.history.length > 0
      end
    end
    t1.join
    t2.join
  end

  def test_bar_nested
    Log::ProgressBar.with_bar(20, :desc => "Bar 1") do |bar|
      bar.init
      20.times do
        Log::ProgressBar.with_bar(5, :desc => "Bar 2") do |bar|
          5.times do
            bar.tick
            sleep 0.2
          end
        end
        bar.tick
        sleep 0.2
      end
    end

  end

end

