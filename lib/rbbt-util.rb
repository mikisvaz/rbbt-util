require_relative 'rbbt'
require_relative 'rbbt/util/misc'
require 'scout/open'
Open.remote_cache_dir = Rbbt.var.cache["open-remote"].find :user

require 'scout/path'

require 'set'
require 'scout/persist'

require 'scout/resource'

require_relative 'rbbt/util/filecache'

require_relative 'rbbt/util/tmpfile'

require_relative 'rbbt/util/cmd'
require_relative 'rbbt/tsv'

require_relative 'rbbt/workflow'

Persist.cache_dir     = Rbbt.var.cache.persistence
FileCache.cachedir    = Rbbt.var.cache.filecache.find
TmpFile.tmpdir        = Rbbt.tmp.find

default_resource = Scout::Config.get(:default_resource, :scout, :rbbt, env: 'DEFAULT_RESOURCE', default: 'rbbt')
case default_resource.to_s.downcase
when 'rbbt'
  Resource.default_resource = Rbbt
else
end

