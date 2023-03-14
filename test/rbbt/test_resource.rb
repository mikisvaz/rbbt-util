require File.expand_path(File.dirname(__FILE__) + '/../test_helper')
require 'rbbt'
require 'rbbt/resource'
require 'rbbt/util/tmpfile'
require 'test/unit'

module TestResource
  extend Resource

  claim tmp.test.google, :url, "http://google.com"
  claim tmp.test.string, :string, "TEST"
  claim tmp.test.proc, :proc do
    "PROC TEST"
  end

  claim tmp.test.rakefiles.foo , :string , <<-EOF
file 'foo' do |t|
  Open.write(t.name, "TEST")
end

rule /.*/ do |t|
  Open.write(t.name, "bar")
end
  EOF

  claim tmp.test.work.footest, :rake, TestResource.tmp.test.rakefiles.foo

  claim tmp.test.work.file_proc, :file_proc do |file,filename|
    Open.write(filename, file)
    nil
  end
end

class TestTSV < Test::Unit::TestCase

  def test_tsv
    require 'rbbt/tsv'
    TestResource.claim TestResource.tmp.test.test_tsv, :proc do 
      tsv = {"a" => 1, "b" => 2}
      TSV.setup(tsv, :key_field => "Letter", :fields => ["Value"], :type => :single)
    end
    assert TSV === TestResource.tmp.test.test_tsv.tsv
  end

  def test_rake
    assert_equal TestResource.tmp.test.work.footest.foo.read, "TEST"
    assert_equal TestResource.tmp.test.work.footest.bar.read, "bar"
  end


  def test_proc
    assert TestResource.tmp.test.proc.read == "PROC TEST"
  end

  def test_string
    assert TestResource.tmp.test.string.read == "TEST"
  end

  def test_url
    assert TestResource[].tmp.test.google.read =~ /google/
  end


  def __test_resolve
    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Path.setup('etc/foo').find(:user)
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Path.setup('etc/foo', 'phgx').find(:user)

    assert_equal File.join('/', 'etc/rbbt/foo'), Path.setup('etc/foo').find(:global)
    assert_equal File.join('/', 'etc/phgx/foo'), Path.setup('etc/foo', 'phgx').find(:global)

    assert_equal File.join('/usr/local', 'etc/rbbt/foo'), Path.setup('etc/foo').find(:local)
    assert_equal File.join('/usr/local', 'etc/phgx/foo'), Path.setup('etc/foo', 'phgx').find(:local)

    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../', 'etc/foo')), Path.setup('etc/foo').find(:lib)
    assert_equal File.expand_path(File.join(File.dirname(File.expand_path(__FILE__)), '../../', 'etc/foo')), Path.setup('etc/foo').find(:lib)

    assert_equal File.join(ENV['HOME'], '.rbbt/etc/foo'), Path.setup('etc/foo').find()
    assert_equal File.join(ENV['HOME'], '.phgx/etc/foo'), Path.setup('etc/foo', 'phgx').find()
  end

  def test_libdir
    assert File.exist?(TestResource[].share.Rlib["util.R"].find(:lib))
    assert File.exist?(TestResource[].share.Rlib["util.R"].find)
  end

  def __test_server
    require 'rbbt/sources/organism'
    TmpFile.with_file :extension => 'gz' do |tmp|
      Organism.get_from_server("Hsa/b37/known_sites/dbsnp_138.vcf.gz", tmp, 'http://rbbt.bsc.es')
      Open.open(tmp) do |f|
        assert f.gets =~  /^#/
      end
    end
  end

  def test_identify
    assert_equal 'etc/', Rbbt.identify(File.join(ENV["HOME"], '.rbbt/etc/'))
    assert_equal 'share/databases/', Rbbt.identify('/usr/local/share/rbbt/databases/')
    assert_equal 'share/databases/DATABASE', Rbbt.identify('/usr/local/share/rbbt/databases/DATABASE')
    assert_equal 'share/databases/DATABASE/FILE', Rbbt.identify('/usr/local/share/rbbt/databases/DATABASE/FILE')
    assert_equal 'share/databases/DATABASE/FILE', Rbbt.identify(File.join(ENV["HOME"], '.rbbt/share/databases/DATABASE/FILE'))
    assert_equal 'share/databases/DATABASE/FILE', Rbbt.identify('/usr/local/share/rbbt/databases/DATABASE/FILE')
  end

end
