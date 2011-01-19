require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "rbbt-util"
    gem.summary = %Q{Utilities for the Ruby Bioinformatics Toolkit (rbbt)}
    gem.description = %Q{Utilities for handling tsv files, caches, etc}
    gem.email = "miguel.vazquez@fdi.ucm.es"
    gem.homepage = "http://github.com/mikisvaz/rbbt-util"
    gem.authors = ["Miguel Vazquez"]
    gem.files = Dir['lib/**/*.rb', 'share/**/*.rb', 'share/**/Rakefile', 'share/lib/R/*.R', 'share/install/software/lib/install_helpers','LICENSE']
    gem.executables = ['tsv.rb', 'tchash.rb']
    gem.test_files = Dir['test/**/test_*.rb']

    
    gem.add_dependency('rake')
    gem.add_dependency('spreadsheet')
    gem.add_dependency('ruby-prof')
    gem.add_dependency('tokyocabinet')
    gem.add_dependency('progress-monitor')

    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
  end
  Jeweler::GemcutterTasks.new  
rescue LoadError
  puts "Jeweler (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
end

begin
  require 'rcov/rcovtask'
  Rcov::RcovTask.new do |test|
    test.libs << 'test'
    test.pattern = 'test/**/test_*.rb'
    test.verbose = true
  end
rescue LoadError
  task :rcov do
    abort "RCov is not available. In order to run rcov, you must: sudo gem install spicycode-rcov"
  end
end

task :test => :check_dependencies

task :default => :test

require 'rake/rdoctask'
Rake::RDocTask.new do |rdoc|
  version = File.exist?('VERSION') ? File.read('VERSION') : ""

  rdoc.rdoc_dir = 'rdoc'
  rdoc.title = "rbbt #{version}"
  rdoc.rdoc_files.include('README*')
  rdoc.rdoc_files.include('lib/**/*.rb')
end
