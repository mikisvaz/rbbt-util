require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R'
require 'rbbt/util/R/plot'

class TestRPlot < Test::Unit::TestCase
  def __test_ggplotgif

    Log.severity = 0
    x = R.eval_a 'rnorm(100, 0, 1)'
    data = TSV.setup(x, "Num~#:type=:single#:cast=:to_f")

    data.add_field "Val" do |k,v|
      k
    end

    R::GIF.eog(data, (0..10).to_a.collect{|i| 100.0 / 10**i}, <<-EOF, nil, nil, :delay => 100)
rbbt.require('reshape')
ggplot(data) + geom_density(bw=frame.value, aes(x=Val))
    EOF

  end

  def test_gif

    Log.severity = 0
    x = R.eval_a 'rnorm(100, 0, 1)'
    data = TSV.setup(x, "Num~#:type=:single#:cast=:to_f")

    data.add_field "Val" do |k,v|
      k
    end

    R::GIF.eog_plot(data, (0..10).to_a.collect{|i| 100.0 / 10**i}, <<-EOF, 400, 400, :delay => 10)
rbbt.require('reshape')
plot(density(data$Val, bw=frame.value))
    EOF

  end
end

