require 'rbbt/resource'

module Rbbt
  extend Resource
  pkgdir = 'rbbt'
end

#Open.cachedir        = Rbbt.var.cache["open-remote"].find :user
#TmpFile.tmpdir       = Rbbt.tmp.find :user
#FileCache.cachedir   = Rbbt.var.cache.filecache.find :user
#Persist.cachedir     = Rbbt.var.cache.persistence.find :user

