require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R/model'

class TestRModel < Test::Unit::TestCase
  def model
  end

  def _test_fit
    data = TSV.setup({}, :key_field => "Dose", :fields => ["Response"], :type => :single)
    10.times do 
      x = rand(10)
      y = 10 + 3 * x + rand * 4
      data[x] = y
    end

    model = R::Model.new "Test fit 2", "Response ~ Dose"

    model.fit(data, method='drm', :fct => "LL.3()") unless model.exists?

    x = 5
    y = 10 + 3 * x 
    input = TSV.setup({"new 1" => [x]}, :key_field => "Code", :fields => ["Dose"], :type => :single)
    puts model.predict(input).to_s
    pred = model.predict(input)["new 1"]["Prediction"].to_f
    assert pred > y and pred < y + 4
  end

  def test_add_fit
    tsv = TSV.open datafile_test('dose_response'), :type => :list
    tsv = tsv.slice(["Dose", "Response"])

    result = tsv.R <<-EOF, :R_debug => true
library(drc, quietly=T)
library(txtplot)
data = rbbt.model.add_fit(data, Response ~ Dose, method=drm, classes='numeric', fct=LL.4(),na.action=na.omit)
txtplot(data$Dose, data$Response)
txtplot(data$Dose, data$Prediction)
    EOF
    ppp result
  end

  def _test_add_inpute
    tsv = TSV.open datafile_test('dose_response'), :type => :list

    result = tsv.R <<-EOF, :R_debug => true
library(drc, quietly=T)
data = rbbt.model.inpute(data, CI ~ Dose, method=drm, classes='numeric', fct=LL.4(), na.action=na.exclude)
    EOF

    assert_equal result.size, result.column("CI").values.flatten.reject{|p| p.nil? or p.empty? or p == "NA"}.length
  end
end
