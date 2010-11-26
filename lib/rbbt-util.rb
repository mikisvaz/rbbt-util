require 'rbbt'
require 'rbbt/util/open'
require 'rbbt/util/cmd'
require 'rbbt/util/tmpfile'
require 'rbbt/util/filecache'
require 'rbbt/util/tsv'
require 'rbbt/util/cachehelper'
require 'rbbt/util/misc'

FileCache.cache_dir = Rbbt.cachedir
Open.cache_dir      = File.join(Rbbt.cachedir, 'open-remote/')
TmpFile.tmp_dir     = File.join(Rbbt.tmpdir)
TSV.cache_dir       = File.join(Rbbt.cachedir, 'tsv_cache')
