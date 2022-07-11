require File.expand_path(File.dirname(__FILE__) + '/../../../test_helper')
require 'test/unit'
require 'rbbt/util/misc/omics'

class TestMiscOmics < Test::Unit::TestCase

  def test_translate_dna_mutation_hgvs2rbbt
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.395G>A"), "A"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1-124C>T"), "T"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.639+6T>A"), "A"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.3386-2A>G"), "G"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.4090_4091insT"), "+T"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.533_534insGGGG"), "+GGGG"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.4249_4249delC"), "-"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.464-2_465delAGTG"), "----"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.209+1delGTAA"), "----"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.371_397del27"), "---------------------------"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1359+2_1359+11delTTAGAAGAGC"), "----------"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.ins?"), "?(c.ins?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?ins?"), "?(c.?_?ins?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.184_185ins?"), "?(c.184_185ins?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?ins57"), "+NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?insCT"), "+CT"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?del?"), "?(c.?_?del?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?"), "?(c.?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(312)?"), "?(c.(312)?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(1669_1671)>?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1050-?"), "?(c.1050-?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(1347_1540)?"), "?(c.(1347_1540)?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1-?_850+?del"), "?(c.1-?_850+?del)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.2289_?del(7236)"), "?(c.2289_?del(7236))"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(3916_3927)del?"), "?(c.(3916_3927)del?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1705_?del?"), "?(c.1705_?del?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1-?_421+?del"), "?(c.1-?_421+?del)"
  end

  def test_translate_prot_mutation_hgvs2rbbt
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.E255K"), "E255K"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.E279Z"), "E279Z"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.R132?"), "R132?"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.*757?"), "*757?"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.A2216>?"), "A2216Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.M552_W557>Z"), "M552Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.T1151_L1152insT"), "T1151Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.N771_P772ins?"), "N771Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.T310_?insKAAQRGA"), "T310Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.D579del"), "D579Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.E746_A750delELREA"), "E746Indel"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.P14fs*?"), "P14Frameshift"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.*588fs?"), "*588Frameshift"
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?fs*(46_47)"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.(A443)fs*?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.(574_1542)fs*?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?fs"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.fs*?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?fs*?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.(P1249)fs*?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.WQQSYLD25?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.(449_514)?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.0?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?_?ins?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?_?insXXXX"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.(A775)ins?"), nil
    assert_equal Misc.translate_prot_mutation_hgvs2rbbt("p.?del"), nil
  end

  def test_index_BED
    text= ""

    %w(1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 X Y MT).each do |chr|
      %w(1 2 3 4 5 6 7 8).each do |i|
        start = i.to_i * 100
        eend = start + 50
        id = [chr, i] * ":"
        text << [chr, start.to_s, eend.to_s, id] * "\t" + "\n"
      end
    end

    io = Misc.open_pipe do |sin|
      sin.write text
    end

    TmpFile.with_file do |dir|
      index = Misc.index_BED(io, dir)
      assert_equal ["1:1"], index["1:120:130"]
      index = Misc.index_BED(io, dir)
      assert_equal ["2:2"], index["2:220:230"]
    end
  end

  def test_sort_genomic_locations
    mutations =<<-EOF.split("\n").shuffle
1:100:A
1:20:A
1:300:A
2:100:A
2:20:A
2:300:A
10:100:A
10:20:A
10:300:A
    EOF
    sorted =  Misc.sort_mutation_stream(StringIO.new(mutations * "\n")).read.split("\n")
    strict_sorted = Misc.sort_mutation_stream_strict(StringIO.new(mutations * "\n")).read.split("\n")

    assert sorted.index("1:20:A") < sorted.index("1:100:A")
    assert sorted.index("1:300:A") < sorted.index("10:300:A")
    assert sorted.index("10:300:A") < sorted.index("2:300:A")
    assert strict_sorted.index("1:20:A") < strict_sorted.index("1:100:A")
    assert strict_sorted.index("1:300:A") < strict_sorted.index("10:300:A")
    assert strict_sorted.index("2:300:A") < strict_sorted.index("10:300:A")
  end
end
