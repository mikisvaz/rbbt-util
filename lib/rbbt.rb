require 'rbbt/util/open'
require 'rbbt/util/tmpfile'
require 'rbbt/util/filecache'
require 'rbbt/tsv'
require 'rbbt/persist'
require 'rbbt/resource'

module Rbbt
  extend Resource
  pkgdir = 'rbbt'

  FileCache.cachedir   = var.cache.filecache.find :user
  TmpFile.tmpdir       = tmp.find :user
  Open.cachedir        = var.cache["open-remote"].find :user
  Persist.cachedir = var.cache.persistence.find :user
end
