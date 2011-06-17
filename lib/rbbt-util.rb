require 'rbbt'
require 'rbbt/util/open'
require 'rbbt/util/cmd'
require 'rbbt/util/tmpfile'
require 'rbbt/util/filecache'
require 'rbbt/util/tsv'
require 'rbbt/util/persistence'
require 'rbbt/util/misc'

FileCache.cachedir   = Rbbt.var.cache.filecache.find :user
Open.cachedir        = Rbbt.var.cache["open-remote"].find :user
TmpFile.tmpdir       = Rbbt.tmp.find :user
Persistence.cachedir = Rbbt.var.cache.persistence.find :user
