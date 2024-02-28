require 'rubygems'
require 'rake'

begin
  require 'jeweler'
  Jeweler::Tasks.new do |gem|
    gem.name = "rbbt-util"
    gem.summary = %Q{Utilities for the Ruby Bioinformatics Toolkit (rbbt)}
    gem.description = %Q{Utilities for handling tsv files, caches, etc}
    gem.email = "miguel.vazquez.g@bsc.es"
    gem.homepage = "http://github.com/mikisvaz/rbbt-util"
    gem.authors = ["Miguel Vazquez"]
    gem.files = Dir['lib/**/*.rb', 'share/**/*.rb', 'share/**/Rakefile', 'share/rbbt_commands/pbs/*', 'share/rbbt_commands/slurm/*', 'share/rbbt_commands/lsf/*', 'share/rbbt_commands/**/*', 'share/*.ru', 'share/Rlib/*.R', 'share/color/*', 'share/install/software/*', 'share/install/software/lib/install_helpers', 'LICENSE', 'bin/rbbt_commands/*', 'etc/app.d/*', 'python/**/*.py']
    gem.executables = ['rbbt_query.rb', 'rbbt_exec.rb', 'rbbt_Rutil.rb', 'rbbt', 'rbbt_dangling_locks.rb', 'rbbt_find.rb']
    gem.test_files = Dir['test/**/test_*.rb']

    
    gem.add_dependency('rake')
    gem.add_dependency('lockfile')
    gem.add_dependency('highline')
    gem.add_dependency('bio-bgzf')
    gem.add_dependency('term-ansicolor')
    gem.add_dependency('to_regexp')
    gem.add_dependency('nakayoshi_fork')
    gem.add_dependency('method_source')
    gem.add_dependency('net')
    #gem.add_dependency('nokogiri')
    #gem.add_dependency('spreadsheet')
    #gem.add_dependency('rubyXL')
    #gem.add_dependency('ruby-prof')
    #gem.add_dependency('RubyInline')
    #gem.add_dependency('rest-client')
    
    # gem is a Gem::Specification... see http://www.rubygems.org/read/chapter/20 for additional settings
    gem.license = "MIT"
  end
  Jeweler::GemcutterTasks.new  
rescue LoadError
  puts "Juwelier (or a dependency) not available. Install it with: sudo gem install jeweler"
end

require 'rake/testtask'
Rake::TestTask.new(:test) do |test|
  test.libs << 'lib' << 'test'
  test.pattern = 'test/**/test_*.rb'
  test.verbose = true
  test.warning = false
end

%w(tsv persist util workflow entity annotations association knowledge_base resource hpc resource concurrency).each do |subsystem|
  Rake::TestTask.new("test_#{subsystem}") do |test|
    test.libs << 'lib' << 'test'
    test.pattern = ["test/rbbt/#{subsystem}/**/*.rb", "test/**/test_#{subsystem}.rb"]
    test.verbose = true
    test.warning = false
  end
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
