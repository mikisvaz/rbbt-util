require 'rbbt'
require 'rbbt/util/misc'
require 'rbbt/util/open'
Open.cachedir        = Rbbt.var.cache["open-remote"].find :user

require 'rbbt/persist'
Persist.cachedir = Rbbt.var.cache.persistence

require 'rbbt/util/filecache'
FileCache.cachedir   = Rbbt.var.cache.filecache.find :user

require 'rbbt/util/tmpfile'
TmpFile.tmpdir       = Rbbt.tmp.find :user

require 'rbbt/util/cmd'
require 'rbbt/tsv'

require 'rbbt/util/config'
