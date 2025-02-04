require 'rbbt'

require 'inline'

# From: https://github.com/mengyao/Complete-Striped-Smith-Waterman-Library
# Citation:  SSW Library: An SIMD Smith-Waterman C/C++ Library for Use in Genomic Applications
#            Mengyao Zhao, Wan-Ping Lee, Gabor T. Marth 
#            http://arxiv.org/abs/1208.6350
module SmithWaterman

  inline(:C) do |builder|
    prefix =<<-EOF
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include #{'"' + Rbbt.share.software.opt.ssw["ssw.h"].find + '"'}
#include #{'"' + Rbbt.share.software.opt.ssw["ssw.c"].find + '"'}
    EOF

    prefix +=<<-'EOF'
void ssw_write (s_align* a, 
      char* ref_seq,
      char* read_seq,
      int8_t* table,
      int fd) { 

  int max_length = 1000000;
  dprintf(fd, "optimal_alignment_score: %d\tsub-optimal_alignment_score: %d\t", a->score1, a->score2);
  if (a->ref_begin1 + 1) dprintf(fd, "target_begin: %d\t", a->ref_begin1 + 1);
  dprintf(fd, "target_end: %d\t", a->ref_end1 + 1);
  if (a->read_begin1 + 1) dprintf(fd, "query_begin: %d\t", a->read_begin1 + 1);
  dprintf(fd, "query_end: %d\n\n", a->read_end1 + 1);
  if (a->cigar) {
    int32_t i, c = 0, left = 0, e = 0, qb = a->ref_begin1, pb = a->read_begin1;
    while (e < a->cigarLen || left > 0) {
      int32_t count = 0;
      int32_t q = qb;
      int32_t p = pb;
      dprintf(fd, "Target: %8d    ", q + 1);
      for (c = e; c < a->cigarLen; ++c) {
        int32_t letter = 0xf&*(a->cigar + c);
        int32_t length = (0xfffffff0&*(a->cigar + c))>>4;
        int32_t l = (count == 0 && left > 0) ? left: length;
        for (i = 0; i < l; ++i) {
          if (letter == 1) dprintf(fd, "-");
          else {
            dprintf(fd, "%c", *(ref_seq + q));
            ++ q;
          }
          ++ count;
          if (count == max_length) goto step2;
        }
      }
step2:
      dprintf(fd, "    %d\n                    ", q);
      q = qb;
      count = 0;
      for (c = e; c < a->cigarLen; ++c) {
        int32_t letter = 0xf&*(a->cigar + c);
        int32_t length = (0xfffffff0&*(a->cigar + c))>>4;
        int32_t l = (count == 0 && left > 0) ? left: length;
        for (i = 0; i < l; ++i){ 
          if (letter == 0) {
            if (table[(int)*(ref_seq + q)] == table[(int)*(read_seq + p)])dprintf(fd, "|");
            else dprintf(fd, "*");
            ++q;
            ++p;
          } else {
            dprintf(fd, "*");
            if (letter == 1) ++p;
            else ++q;
          }
          ++ count;
          if (count == max_length) {
            qb = q;
            goto step3;
          }
        }
      }
step3:
      p = pb;
      dprintf(fd, "\nQuery:  %8d    ", p + 1);
      count = 0;
      for (c = e; c < a->cigarLen; ++c) {
        int32_t letter = 0xf&*(a->cigar + c);
        int32_t length = (0xfffffff0&*(a->cigar + c))>>4;
        int32_t l = (count == 0 && left > 0) ? left: length;
        for (i = 0; i < l; ++i) { 
          if (letter == 2) dprintf(fd, "-");
          else {
            dprintf(fd, "%c", *(read_seq + p));
            ++p;
          }
          ++ count;
          if (count == max_length) {
            pb = p;
            left = l - i - 1;
            e = (left == 0) ? (c + 1) : c;
            goto end;
          }
        }
      }
      e = c;
      left = 0;
end:
      dprintf(fd, "    %d\n\n", p);
    }
  }
}

    EOF

    builder.prefix prefix


    script = <<-EOF
  int ssw_nt(char * read_seq, char * ref_seq){
    int32_t l, m, k, match = 2, mismatch = 2, gap_open = 3, gap_extension = 1;	// default parameters for genome sequence alignment
    // reference sequence
    //char ref_seq[40] = {'C', 'A', 'G', 'C', 'C', 'T', 'T', 'T', 'C', 'T', 'G', 'A', 'C', 'C', 'C', 'G', 'G', 'A', 'A', 'A', 'T', 
    //					'C', 'A', 'A', 'A', 'A', 'T', 'A', 'G', 'G', 'C', 'A', 'C', 'A', 'A', 'C', 'A', 'A', 'A', '\0'};	
    //char read_seq[16] = {'C', 'T', 'G', 'A', 'G', 'C', 'C', 'G', 'G', 'T', 'A', 'A', 'A', 'T', 'C', '\0'};	// read sequence

    s_profile* profile;
    int8_t* num = (int8_t*)malloc(16);	// the read sequence represented in numbers
    int8_t* ref_num = (int8_t*)malloc(64);	// the read sequence represented in numbers
    s_align* result;

    /* This table is used to transform nucleotide letters into numbers. */
    int8_t nt_table[128] = {
      4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4,
      4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 0, 4, 1,  4, 4, 4, 2,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 4, 4, 4,  3, 0, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 0, 4, 1,  4, 4, 4, 2,  4, 4, 4, 4,  4, 4, 4, 4, 
      4, 4, 4, 4,  3, 0, 4, 4,  4, 4, 4, 4,  4, 4, 4, 4 
    };

    // initialize scoring matrix for genome sequences
    //  A  C  G  T	N (or other ambiguous code) 
    //  2 -2 -2 -2 	0	A
    // -2  2 -2 -2 	0	C
    // -2 -2  2 -2 	0	G
    // -2 -2 -2  2 	0	T
    //	0  0  0  0  0	N (or other ambiguous code)	
    int8_t* mat = (int8_t*)calloc(25, sizeof(int8_t));
    for (l = k = 0; l < 4; ++l) {
      for (m = 0; m < 4; ++m) mat[k++] = l == m ? match : - mismatch;	/* weight_match : -weight_mismatch */
      mat[k++] = 0; // ambiguous base: no penalty
    }
    for (m = 0; m < 5; ++m) mat[k++] = 0;

    for (m = 0; m < 15; ++m) num[m] = nt_table[(int)read_seq[m]];
    profile = ssw_init(num, 15, mat, 5, 2);
    for (m = 0; m < 39; ++m) ref_num[m] = nt_table[(int)ref_seq[m]];

    // Only the 8 bit of the flag is setted. ssw_align will always return the best alignment beginning position and cigar.
    result = ssw_align (profile, ref_num, 39, gap_open, gap_extension, 1, 0, 0, 15);	
    //ssw_write(result, ref_seq, read_seq, nt_tablte);

    free(mat);
    free(ref_num);
    free(num);
    return(0);
  }

    EOF
    builder.c_singleton script

    script = <<-EOF
  int ssw_aa(char * read_seq, char * ref_seq, int read_seq_len, int ref_seq_len, int fd){

    int32_t l, m, k, match = 2, mismatch = 2, gap_open = 3, gap_extension = 1;	// default parameters for genome sequence alignment

    s_profile* profile;
    int8_t* num = (int8_t*)malloc(read_seq_len);	// the read sequence represented in numbers
    int8_t* ref_num = (int8_t*)malloc(ref_seq_len);	// the reference sequence represented in numbers
    s_align* result;

    /* This table is used to transform amino acid letters into numbers. */
    int8_t aa_table[128] = {
      23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 
      23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 
      23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23,
      23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 23, 
      23, 0,  20, 4,  3,  6,  13, 7,  8,  9,  23, 11, 10, 12, 2,  23, 
      14, 5,  1,  15, 16, 23, 19, 17, 22, 18, 21, 23, 23, 23, 23, 23, 
      23, 0,  20, 4,  3,  6,  13, 7,  8,  9,  23, 11, 10, 12, 2,  23, 
      14, 5,  1,  15, 16, 23, 19, 17, 22, 18, 21, 23, 23, 23, 23, 23 
    };

    int8_t mat[] = {
    //  A   R   N   D   C   Q   E   G   H   I   L   K   M   F   P   S   T   W   Y   V   B   Z   X   *   
        5, -2, -1, -2, -1, -1, -1,  0, -2, -1, -2, -1, -1, -3, -1,  1,  0, -3, -2,  0, -2, -1, -1, -5,	// A
         -2,  7, -1, -2, -4,  1,  0, -3,  0, -4, -3,  3, -2, -3, -3, -1, -1, -3, -1, -3, -1,  0, -1, -5,	// R
         -1, -1,  7,  2, -2,  0,  0,  0,  1, -3, -4,  0, -2, -4, -2,  1,  0, -4, -2, -3,  5,  0, -1, -5,	// N
         -2, -2,  2,  8, -4,  0,  2, -1, -1, -4, -4, -1, -4, -5, -1,  0, -1, -5, -3, -4,  6,  1, -1, -5,	// D
         -1, -4, -2, -4, 13, -3, -3, -3, -3, -2, -2, -3, -2, -2, -4, -1, -1, -5, -3, -1, -3, -3, -1, -5,	// C
         -1,  1,  0,  0, -3,  7,  2, -2,  1, -3, -2,  2,  0, -4, -1,  0, -1, -1, -1, -3,  0,  4, -1, -5,	// Q
         -1,  0,  0,  2, -3,  2,  6, -3,  0, -4, -3,  1, -2, -3, -1, -1, -1, -3, -2, -3,  1,  5, -1, -5,	// E
        0, -3,  0, -1, -3, -2, -3,  8, -2, -4, -4, -2, -3, -4, -2,  0, -2, -3, -3, -4, -1, -2, -1, -5,	// G
         -2,  0,  1, -1, -3,  1,  0, -2, 10, -4, -3,  0, -1, -1, -2, -1, -2, -3,  2, -4,  0,  0, -1, -5,	// H
         -1, -4, -3, -4, -2, -3, -4, -4, -4,  5,  2, -3,  2,  0, -3, -3, -1, -3, -1,  4, -4, -3, -1, -5,	// I
         -2, -3, -4, -4, -2, -2, -3, -4, -3,  2,  5, -3,  3,  1, -4, -3, -1, -2, -1,  1, -4, -3, -1, -5,	// L
         -1,  3,  0, -1, -3,  2,  1, -2,  0, -3, -3,  6, -2, -4, -1,  0, -1, -3, -2, -3,  0,  1, -1, -5,	// K
         -1, -2, -2, -4, -2,  0, -2, -3, -1,  2,  3, -2,  7,  0, -3, -2, -1, -1,  0,  1, -3, -1, -1, -5,	// M
         -3, -3, -4, -5, -2, -4, -3, -4, -1,  0,  1, -4,  0,  8, -4, -3, -2,  1,  4, -1, -4, -4, -1, -5,	// F
         -1, -3, -2, -1, -4, -1, -1, -2, -2, -3, -4, -1, -3, -4, 10, -1, -1, -4, -3, -3, -2, -1, -1, -5,	// P
        1, -1,  1,  0, -1,  0, -1,  0, -1, -3, -3,  0, -2, -3, -1,  5,  2, -4, -2, -2,  0,  0, -1, -5,	// S
        0, -1,  0, -1, -1, -1, -1, -2, -2, -1, -1, -1, -1, -2, -1,  2,  5, -3, -2,  0,  0, -1, -1, -5, 	// T
         -3, -3, -4, -5, -5, -1, -3, -3, -3, -3, -2, -3, -1,  1, -4, -4, -3, 15,  2, -3, -5, -2, -1, -5, 	// W
         -2, -1, -2, -3, -3, -1, -2, -3,  2, -1, -1, -2,  0,  4, -3, -2, -2,  2,  8, -1, -3, -2, -1, -5, 	// Y
        0, -3, -3, -4, -1, -3, -3, -4, -4,  4,  1, -3,  1, -1, -3, -2,  0, -3, -1,  5, -3, -3, -1, -5, 	// V
         -2, -1,  5,  6, -3,  0,  1, -1,  0, -4, -4,  0, -3, -4, -2,  0,  0, -5, -3, -3,  6,  1, -1, -5, 	// B
         -1,  0,  0,  1, -3,  4,  5, -2,  0, -3, -3,  1, -1, -4, -1,  0, -1, -2, -2, -3,  1,  5, -1, -5, 	// Z
         -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -5, 	// X
         -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5, -5,  1 	// *
    };	


    for (m = 0; m < read_seq_len; ++m) num[m] = aa_table[(int)read_seq[m]];
    profile = ssw_init(num, read_seq_len, mat, 24, 2);
    for (m = 0; m < ref_seq_len; ++m) ref_num[m] = aa_table[(int)ref_seq[m]];

    // Only the 8 bit of the flag is setted. ssw_align will always return the best alignment beginning position and cigar.
    result = ssw_align(profile, ref_num, ref_seq_len, gap_open, gap_extension, 1, 0, 0, read_seq_len );	
    ssw_write(result, ref_seq, read_seq, aa_table, fd);

    free(num);
    free(ref_num);
    return(0);
  }

    EOF
    builder.c_singleton script

  end

  def self.align_txt(query_sequence, target_sequence)
    Log.low { "Aligning #{ Log.fingerprint query_sequence } to #{ Log.fingerprint target_sequence }" }

    raise "No query sequence" if query_sequence.nil?
    raise "No target sequence" if target_sequence.nil?

    s_out = Open.open_pipe do |s_in|
      SmithWaterman.ssw_aa(query_sequence, target_sequence, query_sequence.length, target_sequence.length, s_in.fileno)
    end

    txt = s_out.read
    s_out.close
    s_out.join

    txt
  end

  def self.align(query_sequence, target_sequence)
    txt = align_txt(query_sequence, target_sequence)

    begin
      ppp txt if Rbbt::Config.get(:print, :ssw_aligmnent, default: false).to_s == 'true'

      target_start, target, target_end = txt.match(/Target:\s+(\d+)\s+([A-Za-z\-?*]+)\s+(\d+)/).values_at 1, 2, 3

      query_start, query, query_end = txt.match(/Query:\s+(\d+)\s+([A-Za-z\-?*]+)\s+(\d+)/).values_at 1, 2, 3

      [("_" * (query_start.to_i - 1)) + query, ("_" * (target_start.to_i - 1)) + target]
    rescue
      Log.warn("Error in aligmnent: #{$!.message}")
      return ["-", "-"]
    end
  end

  def self.alignment_map(source, target, filter_low_quality=true)
    alignment_source, alignment_target = SmithWaterman.align(source, target)
    map = {}

    offset_source, alignment_source = alignment_source.match(/^(_*)(.*)/).values_at( 1, 2)
    offset_target, alignment_target = alignment_target.match(/^(_*)(.*)/).values_at( 1, 2)

    gaps_source = 0 
    gaps_target = 0
    miss_match = 0
    alignment_source.chars.zip(alignment_target.chars).each_with_index do |p,i|
      char_source, char_target = p
      gaps_source += 1 if char_source == '-'
      gaps_target += 1 if char_target == '-'
      source_pos = i + 1 + offset_source.length - gaps_source
      target_pos = i + 1 + offset_target.length - gaps_target
      if char_source != char_target or char_source == "-"
        miss_match += 1
      else
        map[source_pos] = target_pos 
      end
    end

    if filter_low_quality && miss_match + gaps_source > alignment_source.length.to_f / 2
      {}
    else
      map
    end
  end

  def self.alignment_percent(source, target, filter_low_quality=true)
    alignment_map(source, target,filter_low_quality).keys.length.to_f / source.length
  end
end
