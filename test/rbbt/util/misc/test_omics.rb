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
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.ins?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?ins?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.184_185ins?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?ins57"), "+NNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNNN"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?insCT"), "+CT"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?_?del?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.?"), "?(c.?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(312)?"), "?(c.(312)?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(1669_1671)>?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1050-?"), "?(c.1050-?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(1347_1540)?"), "?(c.(1347_1540)?)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1-?_850+?del"), "c.1-?_850+?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.2289_?del(7236)"), "(7236)"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.(3916_3927)del?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1705_?del?"), "?"
    assert_equal Misc.translate_dna_mutation_hgvs2rbbt("c.1-?_421+?del"), "c.1-?_421+?"
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
end
