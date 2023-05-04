gem "test-unit", "~> 3.0"
gem "minitest", "~> 5.5"

$LOAD_PATH.unshift(File.join(__dir__, '../../..', 'lib'))
$LOAD_PATH.unshift(File.join(__dir__, '..', 'lib'))
$LOAD_PATH.unshift(__dir__)

require 'rbbt'
require 'test/unit'
require 'fileutils'
require 'rubygems'

require 'rbbt'
require 'rbbt/resource/path'
require 'rbbt/util/config'

require 'scout/persist'


class TestServerLoaded < Exception; end

class Test::Unit::TestCase
  include FileUtils

  def setup
    Random.new

    if defined? Persist
      Persist.cache_dir = Rbbt.tmp.test.persistence.find(:user)
    end

    Entity.entity_property_cache = Rbbt.tmp.test.entity_property.find(:user) if defined? Entity
  end

  def teardown
    FileUtils.rm_rf Rbbt.tmp.test.workflow.find
    Open.clear_dir_repos if defined?(Open) && Open.respond_to?(:clear_dir_repos)
    if defined? Persist
      FileUtils.rm_rf Path.setup("", 'rbbt').tmp.test.find :user
      Persist::CONNECTIONS.values.each do |c| c.close end
      Persist::CONNECTIONS.clear
    end

    if defined? Entity
      FileUtils.rm_rf Entity.entity_property_cache.find(:user) if Entity.entity_property_cache =~ /tmp\/test/
    end
  end

  def config(*args)
    Rbbt::Config.get *args
  end

  def keyword_test(key, &block)
    test = config(:test, key)
    if %w(true yes).include?(test.to_s.downcase)
      block.call
    else
      Log.high "Not testing for #{key}"
    end
  end

  def self.datadir_test
    #Rbbt.root.test.data
    Path.setup(File.join(File.dirname(__FILE__), 'data'))
  end

  def self.datafile_test(file)
    datadir_test[file.to_s]
  end

  def datadir_test
    Test::Unit::TestCase.datadir_test
  end


  def datafile_test(file)
    Test::Unit::TestCase.datafile_test(file)
  end

  def workflow_server(workflow, options = {}, &block)
    trap(:USR1){ raise TestServerLoaded}

    begin
      pid = Process.fork do 
        TmpFile.with_file do |app_dir|
          Misc.in_dir(app_dir) do
            require 'rack'
            ENV["RBBT_WORKFLOW_EXPORT_ALL"] = 'true'

            app_dir = Path.setup(app_dir.dup)
            Open.write(app_dir.etc.target_workflow.find, workflow.to_s)

            config_ru_file = File.exist?('./workflow_config.ru') ? './workflow_config.ru' : Rbbt.share['workflow_config.ru'].find
            options[:config] = config_ru_file
            app = Rack::Server.new(options)
            app.start do
              Process.kill :USR1, Process.ppid
            end
          end
        end
      end

      begin
        sleep 1 while true
      rescue TestServerLoaded
      end

      client = WorkflowRemoteClient.new "http://localhost:#{options[:Port] || 9292}/#{workflow.to_s}", workflow.to_s

      yield client

    ensure
      Process.kill :INT, pid
      Process.wait pid
    end
  end

end
