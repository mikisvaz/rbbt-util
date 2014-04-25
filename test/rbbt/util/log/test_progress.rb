require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/log/progress'

class TestProgress < Test::Unit::TestCase
  def _test_bar
    t1 = Thread.new do
      Log::ProgressBar.with_bar(20) do |bar|
        20.times do
          bar.tick
          sleep 0.3
        end
      end
    end

    t2 = Thread.new do
      Log::ProgressBar.with_bar(20) do |bar|
        20.times do
          bar.tick
          sleep 0.2
        end
      end
    end
    t1.join
    t2.join
  end

  def test_bar_no_size
    t1 = Thread.new do
      Log::ProgressBar.with_bar(nil) do |bar|
        20.times do
          bar.tick
          sleep 0.3
        end
      end
    end

    t2 = Thread.new do
      Log::ProgressBar.with_bar(nil) do |bar|
        20.times do
          bar.tick
          sleep 0.2
        end
      end
    end
    t1.join
    t2.join
  end
end

