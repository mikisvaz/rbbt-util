require_relative 'rbbt'
require_relative 'rbbt/util/misc'
require 'scout/open'
Open.remote_cache_dir        = Rbbt.var.cache["open-remote"].find :user

require 'set'
require 'scout/persist'
Persist.cache_dir = Rbbt.var.cache.persistence

require_relative 'rbbt/util/filecache'
FileCache.cachedir   = Rbbt.var.cache.filecache.find :user

require_relative 'rbbt/util/tmpfile'
TmpFile.tmpdir       = Rbbt.tmp.find :user

require_relative 'rbbt/util/cmd'
require_relative 'rbbt/tsv'

require_relative 'rbbt/util/config'
