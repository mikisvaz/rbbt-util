require 'rbbt/util/R'

module R

  class << self
    attr_accessor :model_dir

    def self.model_dir=(model_dir)
      @model_dir = Path === model_dir ? model_dir : Path.setup(model_dir)
    end
    def self.model_dir
      @model_dir ||= Rbbt.var.R.models
    end
  end


  self.model_dir = Rbbt.var.R.models
  class Model
    R_METHOD = :eval

    attr_accessor :name, :formula
    def initialize(name, formula, options = {})
      @name = name
      @formula = formula
      @options = options || {}
    end

    def colClasses(tsv)
      "c('character', " << 
      (tsv.fields.collect{|f| R.ruby2R(@options[f] ? @options[f].to_s : ":NA") } * ", ") <<
      ")"
    end

    def r_options(tsv)
      {:R_open => "colClasses=#{colClasses(tsv)}", 
        :R_method => (@options[:R_method] || R_METHOD), 
          :source => @options[:source]}
    end

    def model_file
      @model_file ||= R.model_dir[Misc.name2basename([name, Misc.name2basename(formula)] * ": ")].find
    end

    def update(tsv, field = "Prediction")
      tsv.R <<-EOF, r_options(tsv)
model = rbbt.model.load('#{model_file}');
model = update(model, data);
save(model, file='#{model_file}');
data = NULL
      EOF
    end

    def self.groom(tsv, formula)
      tsv = tsv.to_list if tsv.type == :single

      if formula.include? tsv.key_field and not tsv.fields.include? tsv.key_field
        tsv = tsv.add_field tsv.key_field do |k,v|
          k
        end
      end

      tsv
    end

    def predict(tsv, field = "Prediction")
      tsv = Model.groom tsv, formula 
      tsv.R <<-EOF, r_options(tsv)
model = rbbt.model.load('#{model_file}');
data.groomed = rbbt.model.groom(data,formula=#{formula})
data$#{field} = predict(model, data.groomed);
      EOF
    end

    def exists?
      File.exists? model_file
    end

    def fit(tsv, method='lm', args = {})
      args_str = ""
      args_str = args.collect{|name,value| [name,R.ruby2R(value)] * "=" } * ", "
      args_str = ", " << args_str unless args_str.empty?

      tsv = Model.groom(tsv, formula)

      FileUtils.mkdir_p File.dirname(model_file) unless File.exists?(File.dirname(model_file))
      tsv.R <<-EOF, r_options(tsv)
model = rbbt.model.fit(data, #{formula}, method=#{method}#{args_str})
save(model, file='#{model_file}')
data = NULL
      EOF
    end
  end
end
