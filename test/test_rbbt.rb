require File.expand_path(File.dirname(__FILE__) + '/test_helper')
require 'rbbt'
require 'rbbt/util/misc'
require 'rbbt/util/open'

class TestRbbt < Test::Unit::TestCase
  def test_url
    Rbbt.claim :test_url, 'http://google.com'
    assert Rbbt.files.test_url.read =~ /html/
    FileUtils.rm Rbbt.files.test_url
  end

  def test_xclip
    sharedir = PKGData.sharedir_for_file(__FILE__)
  
    File.open(File.join(sharedir, 'install', 'software', 'xclip'), 'w') do |f|
      f.puts '#!/bin/bash

INSTALL_HELPER_FILE="$1"
RBBT_SOFTWARE_DIR="$2"
source "$INSTALL_HELPER_FILE"

name="xclip:0.12"
url="http://downloads.sourceforge.net/project/xclip/xclip/0.12/xclip-0.12.tar.gz?r=http%3A%2F%2Fsourceforge.net%2Fprojects%2Fxclip%2F&ts=1286472387&use_mirror=sunet"

install_src "$name" "$url"
      '
    end
    FileUtils.chmod 0774, File.join(sharedir, 'install', 'software', 'xclip')

    Rbbt.add_software :xclip => ['','']
    Rbbt.find_software :xclip
    assert File.exists? File.join(Rbbt.bin_dir, 'xclip')

    FileUtils.rm_rf  File.join(sharedir, 'install', 'software', 'xclip')
    Dir.glob(File.join(Rbbt.datadir, 'software', 'opt', 'xclip','bin/*')).each do |exe|
      FileUtils.rm_rf  File.join(Rbbt.datadir, 'software', 'opt', 'bin', File.basename(exe))
    end
    Dir.glob(File.join(Rbbt.datadir, 'software', 'opt', 'xclip','current', 'bin/*')).each do |exe|
      FileUtils.rm_rf  File.join(Rbbt.datadir, 'software', 'opt', 'bin', File.basename(exe))
    end
    FileUtils.rm_rf  File.join(Rbbt.datadir, 'software', 'opt', 'xclip')
  end

end
