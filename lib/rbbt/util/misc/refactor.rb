require_relative '../../refactor'
require 'scout/misc'
require 'scout/open'
require_relative '../log/refactor'

Rbbt.relay_module_method Misc, :sensiblewrite, Open, :sensible_write
Rbbt.relay_module_method Misc, :file2md5, Misc, :digest_file
Rbbt.relay_module_method Misc, :lock, Open
Rbbt.relay_module_method Misc, :consume_stream, Open
Rbbt.relay_module_method Misc, :sort_stream, Open
Rbbt.relay_module_method Misc, :sanitize_filename, Path
Rbbt.relay_module_method Misc, :collapse_stream, Open
Rbbt.relay_module_method Misc, :open_pipe, Open
Rbbt.relay_module_method Misc, :pipe, Open
Rbbt.relay_module_method Misc, :with_fifo, Open
Rbbt.relay_module_method Misc, :zip2hash, IndiferentHash
Rbbt.relay_module_method Misc, :obj2md5, Misc, :digest
Rbbt.relay_module_method Misc, :obj2digest, Misc, :digest
