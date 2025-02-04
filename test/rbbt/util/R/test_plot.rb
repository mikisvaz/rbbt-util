require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R'
require 'rbbt/util/R/plot'

class TestRPlot < Test::Unit::TestCase
  def _test_ggplotgif

    x = R.eval_a 'rnorm(100, 0, 1)'
    data = TSV.setup(x, "Num~#:type=:single#:cast=:to_f")

    data.add_field "Val" do |k,v|
      k
    end

    R::GIF.eog(data, (0..10).to_a.collect{|i| 100.0 / 10**i}, <<-EOF, nil, nil, :delay => 30)
rbbt.require('reshape')
ggplot(data) + geom_density(bw=frame.value, aes(x=Val))
    EOF

  end

  def _test_gif

    x = R.eval_a 'rnorm(100, 0, 1)'
    data = TSV.setup(x, "Num~#:type=:single#:cast=:to_f")

    data.add_field "Val" do |k,v|
      k
    end

    R::GIF.eog_plot(data, (0..10).to_a.collect{|i| 100.0 / 10**i}, <<-EOF, 400, 400, :delay => 30)
rbbt.require('reshape')
plot(density(data$Val, bw=frame.value))
    EOF
  end
  
  def test_ggplot_entity

    x = R.eval_a 'rnorm(100, 0, 1)'
    data = TSV.setup(x, "Num~#:type=:list")

    data.add_field "Val" do |k,v|
      k
    end

    data.add_field "Entity" do |k,v|
      ["Entity", rand(100000).to_s] * ""
    end

    data.add_field "Entity type" do |k,v|
      "RandomEntity"
    end

    data = data.reorder "Entity", ["Entity", "Entity type", "Val"]
    data.key_field = "Key"
    Log.tsv data

    svg = R::SVG.ggplot(data, <<-EOF, nil, nil, entity_geom: 'geom_point')
        ggplot(data) + geom_point(aes(x=Val, y=Val))
    EOF
    assert svg.include?('geom_point')
    assert svg.include?('data-entity')
  end
end

