require File.join(File.expand_path(File.dirname(__FILE__)), '../..', 'test_helper.rb')
require 'rbbt/util/rake'

class TestRake < Test::Unit::TestCase
  def test_run
    rakefile=<<-EOF
require 'rbbt/util/rake'

file "foo" do |t|
  Open.write(t.name, 'bar')
end
    EOF

    TmpFile.with_file(rakefile) do |f|
      RakeHelper.run f, :foo

      assert File.exists? "foo"
      FileUtils.rm "foo"
    end
  end

  def test_run_default
    rakefile=<<-EOF
require 'rbbt/util/rake'

file "foo" do |t|
  Open.write(t.name, 'bar')
end
    EOF

    TmpFile.with_file(rakefile) do |f|
      RakeHelper.run f

      assert File.exists? "foo"
      FileUtils.rm "foo"
    end
  end


  def test_files
    rakefile=<<-EOF
require 'rbbt/util/rake'

file "foo" do |t|
  Open.write(t.name, 'bar')
end
    EOF

    TmpFile.with_file(rakefile) do |f|
      assert RakeHelper.files(f).include? "foo"
    end
  end
end

