require 'rbbt'
require 'rbbt/util/open'
require 'rbbt/util/cmd'
require 'rbbt/util/tmpfile'
require 'rbbt/util/filecache'
require 'rbbt/util/tsv'
require 'rbbt/util/persistence'
require 'rbbt/util/bed'
require 'rbbt/util/cachehelper'
require 'rbbt/util/misc'

FileCache.cachedir = Rbbt.cachedir
Open.cachedir      = File.join(Rbbt.cachedir, 'open-remote/')
TmpFile.tmpdir     = File.join(Rbbt.tmpdir)
Persistence.cachedir       = File.join(Rbbt.cachedir, 'persistence')
Bed.cachedir       = File.join(Rbbt.cachedir, 'bed_cache')
