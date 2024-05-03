require_relative 'rbbt'
require_relative 'rbbt/util/misc'
require 'scout/open'

require 'scout/path'

require 'set'
require 'scout/persist'

require 'scout/resource'

require_relative 'rbbt/util/filecache'

require_relative 'rbbt/util/tmpfile'

require_relative 'rbbt/util/cmd'
require_relative 'rbbt/tsv'

require_relative 'rbbt/util/config'
require_relative 'rbbt/workflow'

Open.remote_cache_dir = Rbbt.var.cache["open-remote"].find :user
Path.default_pkgdir   = Rbbt
Persist.cache_dir     = Rbbt.var.cache.persistence
FileCache.cachedir    = Rbbt.var.cache.filecache.find :user
TmpFile.tmpdir        = Rbbt.tmp.find :user
Resource.default_resource = Rbbt


class << Workflow
  def workflow_dir
    @workflow_dir || 
      ENV["RBBT_WORKFLOW_DIR"] || 
      begin 
        workflow_dir_config = Path.setup("etc/workflow_dir")
        if workflow_dir_config.exists?
          Path.setup(workflow_dir_config.read.strip)
        else
          Path.setup('workflows').find(:user)
        end
      end
  end

  def workflow_repo
    @workflow_repo || 
      ENV["RBBT_WORKFLOW_REPO"] || 
      begin 
        workflow_repo_config = Path.setup("etc/workflow_repo")
        if workflow_repo_config.exists?
          workflow_repo_config.read.strip
        else
          'https://github.com/Rbbt-Workflows/'
        end
      end
  end
end
