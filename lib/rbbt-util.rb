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
