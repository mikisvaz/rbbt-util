require File.join(File.expand_path(File.dirname(__FILE__)), '../../..', 'test_helper.rb')
require 'rbbt/util/R'
require 'rbbt/util/R/model'

class TestRModel < Test::Unit::TestCase
  def model
  end

  def test_fit
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
    pred = model.predict(input)["new 1"]["Prediction"].to_f
    assert pred > y and pred < y + 4
  end

  def test_add_fit
    tsv = TSV.open datafile_test('dose_response'), :type => :list
    tsv = tsv.slice(["Dose", "Response"])

    result = tsv.R <<-EOF, :R_method => :shell
library(drc, quietly=T)
library(txtplot)
data = rbbt.model.add_fit(data, Response ~ Dose, method=drm, classes='numeric', fct=LL.4(),na.action=na.omit)
txtplot(data$Dose, data$Response)
txtplot(data$Dose, data$Prediction)
    EOF
    assert result.fields.include? "Prediction"
  end

  def test_add_inpute
    tsv = TSV.open datafile_test('dose_response'), :type => :list

    result = tsv.R <<-EOF, :R_method => :eval
library(drc, quietly=T)
data = rbbt.model.inpute(data, CI ~ Dose, method=drm, classes='numeric', fct=LL.4(), na.action=na.exclude)
    EOF

    assert_equal result.size, result.column("CI").values.flatten.reject{|p| p.nil? or p.empty? or p == "NA"}.length
  end

  def __test_ab_surv_corr
    require 'rbbt/workflow'
    Workflow.require_workflow "Miller"

    require 'rbbt/util/R/model'
    require 'rbbt/util/R/svg'

    antibody = "14-3-3-Zeta(C)_GBL9006927"

    rppa = Miller.RPPA.data.tsv
    rppa.attach Miller.RPPA.labels
    viability = Miller.Viability.data.tsv

    viability.add_field "Perturbation" do |compound,values|
      values["Dose"].collect do |dose|
        compound.split("-").flatten.zip(dose.split("-")).collect{|p| p * "="} * "-"
      end
    end

    viability = viability.reorder "Perturbation", nil, :zipped => true
    compounds = viability.column("Compound").flatten.uniq

    rppa = rppa.select("Compound"){|c| ! c.include? "-" and compounds.include? c}
    rppa = rppa.slice([antibody,"Compound", "Dose"])
    rppa.rename_field antibody, "RPPA"

    model = R::Model.new "viability", "Effect ~ Dose", nil, "Compound" => :factor

    model.fit(viability.select("Compound"){|c| ! c.include? "-"}, 'lm')

    rppa = model.predict(rppa, "Prediction")

    plot_script = "plot<-ggplot(data=data) + geom_point(aes(x=RPPA, y=Prediction, color=Compound));"

    R::SVG.ggplotSVG rppa, plot_script, 7, 7, :R_method => :eval 
  end

  def test_fit_fast
    data = TSV.setup({}, :key_field => "Dose", :fields => ["Response"], :type => :single)
    10.times do 
      x = rand(10)
      y = 10 + 3 * x + rand * 4
      data[x] = y
    end

    model = R::Model.new "Test fit 2", "Response ~ Dose", data, :fit => 'lm'

    x = 5
    y = 10 + 3 * x 

    pred = model.predict x
    assert pred > y and pred < y + 4

    pred = model.predict [x, 2*x, 3*x]
    assert pred.first > y and pred.first  < y + 4


    pred = model.predict "Dose" => x
    assert pred > y and pred < y + 4
  end

  def test_interval
    data = TSV.setup({}, :key_field => "Dose", :fields => ["Response"], :type => :single)
    10.times do 
      x = rand(10)
      y = 10 + 3 * x + rand * 4
      data[x] = y
    end

    model = R::Model.new "Test fit 2", "Response ~ Dose", data, :fit => 'lm'

    x = 5
    y = 10 + 3 * x 

    pred = model.predict x
    assert pred > y and pred < y + 4
  end
end
