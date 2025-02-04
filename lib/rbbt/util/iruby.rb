require 'iruby'
require 'rbbt/util/R'

module IRuby
  def self.img(file)
    IRuby.html("<img src='#{file}'/>")
  end

  def self.plot(...)
    filename = Rbbt.iruby[rand(10000).to_s + ".png"]
    Open.mkdir File.dirname(filename)
    R::PNG.plot(filename, ...)
    img(filename)
  end

  def self.ggplot(...)
    svg = R::SVG.ggplot(...)
    IRuby.html(svg)
  end
end
