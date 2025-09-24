/* ---------- begin ssw.c ---------- */
/* The MIT License

   Copyright (c) 2012-1015 Boston College.

   Permission is hereby granted, free of charge, to any person obtaining
   a copy of this software and associated documentation files (the
   "Software"), to deal in the Software without restriction, including
   without limitation the rights to use, copy, modify, merge, publish,
   distribute, sublicense, and/or sell copies of the Software, and to
   permit persons to whom the Software is furnished to do so, subject to
   the following conditions:

   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.
*/

/* Contact: Mengyao Zhao <zhangmp@bc.edu> */

#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <math.h>
#include "ssw.h"

/* Detect SSE2 availability (x86 or explicitly -msse2) */
#if defined(__SSE2__) || defined(__x86_64__) || defined(_M_X64)
#define HAVE_SSE2 1
#else
#define HAVE_SSE2 0
#endif

#if HAVE_SSE2
#include <emmintrin.h>
#endif

#ifdef __GNUC__
#define LIKELY(x) __builtin_expect((x),1)
#define UNLIKELY(x) __builtin_expect((x),0)
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#define set_u(u, w, i, j) { int x=(i)-(w); x=x>0?x:0; (u)=(j)-x+1; }
#define set_d(u, w, i, j, p) { int x=(i)-(w); x=x>0?x:0; x=(j)-x; (u)=x*3+p; }
#define kroundup32(x) (--(x), (x)|=(x)>>1, (x)|=(x)>>2, (x)|=(x)>>4, (x)|=(x)>>8, (x)|=(x)>>16, ++(x))

typedef struct {
	uint16_t score;
	int32_t ref;	 /* 0-based position */
	int32_t read;    /* alignment ending position on read, 0-based */
} alignment_end;

typedef struct {
	uint32_t* seq;
	int32_t length;
} cigar;

struct _profile{
#if HAVE_SSE2
	__m128i* profile_byte;
	__m128i* profile_word;
#else
	void* profile_byte;
	void* profile_word;
#endif
	const int8_t* read;
	const int8_t* mat;
	int32_t readLen;
	int32_t n;
	uint8_t bias;
};

#if HAVE_SSE2

/* Portable extractor for 16-bit lane: use store+index in case _mm_extract_epi16 is unavailable */
static inline int mm_extract_epi16(__m128i v, int idx)
{
    short tmp[8];
    _mm_storeu_si128((__m128i*)tmp, v);
    return tmp[idx];
}

/* --- Original SSE implementations (unchanged logic). --- */

/* Generate query profile for byte (8-bit) */
__m128i* qP_byte (const int8_t* read_num,
				  const int8_t* mat,
				  const int32_t readLen,
				  const int32_t n,
				  uint8_t bias) {
	int32_t segLen = (readLen + 15) / 16;
	__m128i* vProfile = (__m128i*)malloc(n * segLen * sizeof(__m128i));
	int8_t* t = (int8_t*)vProfile;
	int32_t nt, i, j, segNum;
	for (nt = 0; LIKELY(nt < n); nt ++) {
		for (i = 0; i < segLen; i ++) {
			j = i;
			for (segNum = 0; LIKELY(segNum < 16) ; segNum ++) {
				*t++ = j>= readLen ? bias : mat[nt * n + read_num[j]] + bias;
				j += segLen;
			}
		}
	}
	return vProfile;
}

/* Byte version SW using SSE2 intrinsics */
alignment_end* sw_sse2_byte (const int8_t* ref,
							 int8_t ref_dir,
							 int32_t refLen,
							 int32_t readLen,
							 const uint8_t weight_gapO,
							 const uint8_t weight_gapE,
							 __m128i* vProfile,
							 uint8_t terminate,
	 						 uint8_t bias,
							 int32_t maskLen) {
#define max16(m, vm) (vm) = _mm_max_epu8((vm), _mm_srli_si128((vm), 8)); \
					  (vm) = _mm_max_epu8((vm), _mm_srli_si128((vm), 4)); \
					  (vm) = _mm_max_epu8((vm), _mm_srli_si128((vm), 2)); \
					  (vm) = _mm_max_epu8((vm), _mm_srli_si128((vm), 1)); \
					  (m) = mm_extract_epi16((vm), 0)

	uint8_t max = 0;
	int32_t end_read = readLen - 1;
	int32_t end_ref = -1;
	int32_t segLen = (readLen + 15) / 16;
	uint8_t* maxColumn = (uint8_t*) calloc(refLen, 1);
	int32_t* end_read_column = (int32_t*) calloc(refLen, sizeof(int32_t));
	__m128i vZero = _mm_set1_epi32(0);
	__m128i* pvHStore = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvHLoad = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvE = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvHmax = (__m128i*) calloc(segLen, sizeof(__m128i));
	int32_t i, j;
	__m128i vGapO = _mm_set1_epi8(weight_gapO);
	__m128i vGapE = _mm_set1_epi8(weight_gapE);
	__m128i vBias = _mm_set1_epi8(bias);
	__m128i vMaxScore = vZero;
	__m128i vMaxMark = vZero;
	__m128i vTemp;
	int32_t edge, begin = 0, end = refLen, step = 1;

	if (ref_dir == 1) {
		begin = refLen - 1;
		end = -1;
		step = -1;
	}
	for (i = begin; LIKELY(i != end); i += step) {
		int32_t cmp;
		__m128i e = vZero, vF = vZero, vMaxColumn = vZero;
		__m128i vH = pvHStore[segLen - 1];
		vH = _mm_slli_si128 (vH, 1);
		__m128i* vP = vProfile + ref[i] * segLen;
		__m128i* pv = pvHLoad;
		pvHLoad = pvHStore;
		pvHStore = pv;

		for (j = 0; LIKELY(j < segLen); ++j) {
			vH = _mm_adds_epu8(vH, _mm_load_si128(vP + j));
			vH = _mm_subs_epu8(vH, vBias);
			e = _mm_load_si128(pvE + j);
			vH = _mm_max_epu8(vH, e);
			vH = _mm_max_epu8(vH, vF);
			vMaxColumn = _mm_max_epu8(vMaxColumn, vH);
			_mm_store_si128(pvHStore + j, vH);
			vH = _mm_subs_epu8(vH, vGapO);
			e = _mm_subs_epu8(e, vGapE);
			e = _mm_max_epu8(e, vH);
			_mm_store_si128(pvE + j, e);
			vF = _mm_subs_epu8(vF, vGapE);
			vF = _mm_max_epu8(vF, vH);
			vH = _mm_load_si128(pvHLoad + j);
		}

        j = 0;
        vH = _mm_load_si128 (pvHStore + j);
        vF = _mm_slli_si128 (vF, 1);
        vTemp = _mm_subs_epu8 (vH, vGapO);
		vTemp = _mm_subs_epu8 (vF, vTemp);
		vTemp = _mm_cmpeq_epi8 (vTemp, vZero);
		cmp  = _mm_movemask_epi8 (vTemp);

        while (cmp != 0xffff) {
            vH = _mm_max_epu8 (vH, vF);
			vMaxColumn = _mm_max_epu8(vMaxColumn, vH);
            _mm_store_si128 (pvHStore + j, vH);
            vF = _mm_subs_epu8 (vF, vGapE);
            j++;
            if (j >= segLen) {
                j = 0;
                vF = _mm_slli_si128 (vF, 1);
            }
            vH = _mm_load_si128 (pvHStore + j);
            vTemp = _mm_subs_epu8 (vH, vGapO);
            vTemp = _mm_subs_epu8 (vF, vTemp);
            vTemp = _mm_cmpeq_epi8 (vTemp, vZero);
            cmp  = _mm_movemask_epi8 (vTemp);
        }

		vMaxScore = _mm_max_epu8(vMaxScore, vMaxColumn);
		vTemp = _mm_cmpeq_epi8(vMaxMark, vMaxScore);
		cmp = _mm_movemask_epi8(vTemp);
		if (cmp != 0xffff) {
			uint8_t temp;
			vMaxMark = vMaxScore;
			max16(temp, vMaxScore);
			vMaxScore = vMaxMark;
			if (LIKELY(temp > max)) {
				max = temp;
				if (max + bias >= 255) break;
				end_ref = i;
				for (j = 0; LIKELY(j < segLen); ++j) pvHmax[j] = pvHStore[j];
			}
		}
		max16(maxColumn[i], vMaxColumn);
		if (maxColumn[i] == terminate) break;
	}

	/* Trace the alignment ending position on read. */
	uint8_t *t = (uint8_t*)pvHmax;
	int32_t column_len = segLen * 16;
	for (i = 0; LIKELY(i < column_len); ++i, ++t) {
		int32_t temp;
		if (*t == max) {
			temp = i / 16 + i % 16 * segLen;
			if (temp < end_read) end_read = temp;
		}
	}

	free(pvHmax);
	free(pvE);
	free(pvHLoad);
	free(pvHStore);

	alignment_end* bests = (alignment_end*) calloc(2, sizeof(alignment_end));
	bests[0].score = max + bias >= 255 ? 255 : max;
	bests[0].ref = end_ref;
	bests[0].read = end_read;
	bests[1].score = 0;
	bests[1].ref = 0;
	bests[1].read = 0;

	edge = (end_ref - maskLen) > 0 ? (end_ref - maskLen) : 0;
	for (i = 0; i < edge; i ++) {
		if (maxColumn[i] > bests[1].score) {
			bests[1].score = maxColumn[i];
			bests[1].ref = i;
		}
	}
	edge = (end_ref + maskLen) > refLen ? refLen : (end_ref + maskLen);
	for (i = edge + 1; i < refLen; i ++) {
		if (maxColumn[i] > bests[1].score) {
			bests[1].score = maxColumn[i];
			bests[1].ref = i;
		}
	}

	free(maxColumn);
	free(end_read_column);
	return bests;
#undef max16
}

/* Generate query profile for 16-bit words */
__m128i* qP_word (const int8_t* read_num,
				  const int8_t* mat,
				  const int32_t readLen,
				  const int32_t n) {
	int32_t segLen = (readLen + 7) / 8;
	__m128i* vProfile = (__m128i*)malloc(n * segLen * sizeof(__m128i));
	int16_t* t = (int16_t*)vProfile;
	int32_t nt, i, j;
	int32_t segNum;
	for (nt = 0; LIKELY(nt < n); nt ++) {
		for (i = 0; i < segLen; i ++) {
			j = i;
			for (segNum = 0; LIKELY(segNum < 8) ; segNum ++) {
				*t++ = j>= readLen ? 0 : mat[nt * n + read_num[j]];
				j += segLen;
			}
		}
	}
	return vProfile;
}

/* Word (16-bit) SSE2 SW */
alignment_end* sw_sse2_word (const int8_t* ref,
							 int8_t ref_dir,
							 int32_t refLen,
							 int32_t readLen,
							 const uint8_t weight_gapO,
							 const uint8_t weight_gapE,
						     __m128i* vProfile,
							 uint16_t terminate,
							 int32_t maskLen) {
#define max8(m, vm) (vm) = _mm_max_epi16((vm), _mm_srli_si128((vm), 8)); \
					(vm) = _mm_max_epi16((vm), _mm_srli_si128((vm), 4)); \
					(vm) = _mm_max_epi16((vm), _mm_srli_si128((vm), 2)); \
					(m) = mm_extract_epi16((vm), 0)

	uint16_t max = 0;
	int32_t end_read = readLen - 1;
	int32_t end_ref = 0;
	int32_t segLen = (readLen + 7) / 8;
	uint16_t* maxColumn = (uint16_t*) calloc(refLen, 2);
	int32_t* end_read_column = (int32_t*) calloc(refLen, sizeof(int32_t));
	__m128i vZero = _mm_set1_epi32(0);
	__m128i* pvHStore = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvHLoad = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvE = (__m128i*) calloc(segLen, sizeof(__m128i));
	__m128i* pvHmax = (__m128i*) calloc(segLen, sizeof(__m128i));
	int32_t i, j, k;
	__m128i vGapO = _mm_set1_epi16(weight_gapO);
	__m128i vGapE = _mm_set1_epi16(weight_gapE);
	__m128i vMaxScore = vZero;
	__m128i vMaxMark = vZero;
	__m128i vTemp;
	int32_t edge, begin = 0, end = refLen, step = 1;

	if (ref_dir == 1) {
		begin = refLen - 1;
		end = -1;
		step = -1;
	}
	for (i = begin; LIKELY(i != end); i += step) {
		int32_t cmp;
		__m128i e = vZero, vF = vZero;
		__m128i vH = pvHStore[segLen - 1];
		vH = _mm_slli_si128 (vH, 2);
		__m128i* pv = pvHLoad;
		__m128i vMaxColumn = vZero;
		__m128i* vP = vProfile + ref[i] * segLen;
		pvHLoad = pvHStore;
		pvHStore = pv;

		for (j = 0; LIKELY(j < segLen); j ++) {
			vH = _mm_adds_epi16(vH, _mm_load_si128(vP + j));
			e = _mm_load_si128(pvE + j);
			vH = _mm_max_epi16(vH, e);
			vH = _mm_max_epi16(vH, vF);
			vMaxColumn = _mm_max_epi16(vMaxColumn, vH);
			_mm_store_si128(pvHStore + j, vH);
			vH = _mm_subs_epu16(vH, vGapO);
			e = _mm_subs_epu16(e, vGapE);
			e = _mm_max_epi16(e, vH);
			_mm_store_si128(pvE + j, e);
			vF = _mm_subs_epu16(vF, vGapE);
			vF = _mm_max_epi16(vF, vH);
			vH = _mm_load_si128(pvHLoad + j);
		}

		for (k = 0; LIKELY(k < 8); ++k) {
			vF = _mm_slli_si128 (vF, 2);
			for (j = 0; LIKELY(j < segLen); ++j) {
				vH = _mm_load_si128(pvHStore + j);
				vH = _mm_max_epi16(vH, vF);
				_mm_store_si128(pvHStore + j, vH);
				vH = _mm_subs_epu16(vH, vGapO);
				vF = _mm_subs_epu16(vF, vGapE);
				if (UNLIKELY(! _mm_movemask_epi8(_mm_cmpgt_epi16(vF, vH)))) goto end;
			}
		}
end:
		vMaxScore = _mm_max_epi16(vMaxScore, vMaxColumn);
		vTemp = _mm_cmpeq_epi16(vMaxMark, vMaxScore);
		cmp = _mm_movemask_epi8(vTemp);
		if (cmp != 0xffff) {
			uint16_t temp;
			vMaxMark = vMaxScore;
			max8(temp, vMaxScore);
			vMaxScore = vMaxMark;
			if (LIKELY(temp > max)) {
				max = temp;
				end_ref = i;
				for (j = 0; LIKELY(j < segLen); ++j) pvHmax[j] = pvHStore[j];
			}
		}
		max8(maxColumn[i], vMaxColumn);
		if (maxColumn[i] == terminate) break;
	}

	uint16_t *t = (uint16_t*)pvHmax;
	int32_t column_len = segLen * 8;
	for (i = 0; LIKELY(i < column_len); ++i, ++t) {
		int32_t temp;
		if (*t == max) {
			temp = i / 8 + i % 8 * segLen;
			if (temp < end_read) end_read = temp;
		}
	}

	free(pvHmax);
	free(pvE);
	free(pvHLoad);
	free(pvHStore);

	alignment_end* bests = (alignment_end*) calloc(2, sizeof(alignment_end));
	bests[0].score = max;
	bests[0].ref = end_ref;
	bests[0].read = end_read;
	bests[1].score = 0;
	bests[1].ref = 0;
	bests[1].read = 0;

	edge = (end_ref - maskLen) > 0 ? (end_ref - maskLen) : 0;
	for (i = 0; i < edge; i ++) {
		if (maxColumn[i] > bests[1].score) {
			bests[1].score = maxColumn[i];
			bests[1].ref = i;
		}
	}
	edge = (end_ref + maskLen) > refLen ? refLen : (end_ref + maskLen);
	for (i = edge; i < refLen; i ++) {
		if (maxColumn[i] > bests[1].score) {
			bests[1].score = maxColumn[i];
			bests[1].ref = i;
		}
	}

	free(maxColumn);
	free(end_read_column);
	return bests;
#undef max8
}

#endif /* HAVE_SSE2 */

/* ---------------- common functions (banded SW and seq reverse) ---------------- */

cigar* banded_sw (const int8_t* ref,
				 const int8_t* read,
				 int32_t refLen,
				 int32_t readLen,
				 int32_t score,
				 const uint32_t weight_gapO,
				 const uint32_t weight_gapE,
				 int32_t band_width,
				 const int8_t* mat,
				 int32_t n) {

	uint32_t *c = (uint32_t*)malloc(16 * sizeof(uint32_t)), *c1;
	int32_t i, j, e, f, temp1, temp2, s = 16, s1 = 8, s2 = 1024, l, max = 0;
	int32_t width, width_d, *h_b, *e_b, *h_c;
	int8_t *direction, *direction_line;
	cigar* result = (cigar*)malloc(sizeof(cigar));
	h_b = (int32_t*)malloc(s1 * sizeof(int32_t));
	e_b = (int32_t*)malloc(s1 * sizeof(int32_t));
	h_c = (int32_t*)malloc(s1 * sizeof(int32_t));
	direction = (int8_t*)malloc(s2 * sizeof(int8_t));

	do {
		width = band_width * 2 + 3, width_d = band_width * 2 + 1;
		while (width >= s1) {
			++s1;
			kroundup32(s1);
			h_b = (int32_t*)realloc(h_b, s1 * sizeof(int32_t));
			e_b = (int32_t*)realloc(e_b, s1 * sizeof(int32_t));
			h_c = (int32_t*)realloc(h_c, s1 * sizeof(int32_t));
		}
		while (width_d * readLen * 3 >= s2) {
			++s2;
			kroundup32(s2);
			if (s2 < 0) {
				fprintf(stderr, "Alignment score and position are not consensus.\n");
				exit(1);
			}
			direction = (int8_t*)realloc(direction, s2 * sizeof(int8_t));
		}
		direction_line = direction;
		for (j = 1; LIKELY(j < width - 1); j ++) h_b[j] = 0;
		for (i = 0; LIKELY(i < readLen); i ++) {
			int32_t beg = 0, end = refLen - 1, u = 0, edge;
			j = i - band_width;	beg = beg > j ? beg : j;
			j = i + band_width; end = end < j ? end : j;
			edge = end + 1 < width - 1 ? end + 1 : width - 1;
			f = h_b[0] = e_b[0] = h_b[edge] = e_b[edge] = h_c[0] = 0;
			direction_line = direction + width_d * i * 3;

			for (j = beg; LIKELY(j <= end); j ++) {
				int32_t b, e1, f1, d, de, df, dh;
				set_u(u, band_width, i, j);	set_u(e, band_width, i - 1, j);
				set_u(b, band_width, i, j - 1); set_u(d, band_width, i - 1, j - 1);
				set_d(de, band_width, i, j, 0);
				set_d(df, band_width, i, j, 1);
				set_d(dh, band_width, i, j, 2);

				temp1 = i == 0 ? -weight_gapO : h_b[e] - weight_gapO;
				temp2 = i == 0 ? -weight_gapE : e_b[e] - weight_gapE;
				e_b[u] = temp1 > temp2 ? temp1 : temp2;
				direction_line[de] = temp1 > temp2 ? 3 : 2;

				temp1 = h_c[b] - weight_gapO;
				temp2 = f - weight_gapE;
				f = temp1 > temp2 ? temp1 : temp2;
				direction_line[df] = temp1 > temp2 ? 5 : 4;

				e1 = e_b[u] > 0 ? e_b[u] : 0;
				f1 = f > 0 ? f : 0;
				temp1 = e1 > f1 ? e1 : f1;
				temp2 = h_b[d] + mat[ref[j] * n + read[i]];
				h_c[u] = temp1 > temp2 ? temp1 : temp2;

				if (h_c[u] > max) max = h_c[u];

				if (temp1 <= temp2) direction_line[dh] = 1;
				else direction_line[dh] = e1 > f1 ? direction_line[de] : direction_line[df];
			}
			for (j = 1; j <= u; j ++) h_b[j] = h_c[j];
		}
		band_width *= 2;
	} while (LIKELY(max < score));
	band_width /= 2;

	// trace back
	i = readLen - 1;
	j = refLen - 1;
	e = 0;
	l = 0;
	f = max = 0;
	temp2 = 2;
	while (LIKELY(i > 0)) {
		set_d(temp1, band_width, i, j, temp2);
		switch (direction_line[temp1]) {
			case 1:
				--i;
				--j;
				temp2 = 2;
				direction_line -= width_d * 3;
				f = 0;
				break;
			case 2:
			 	--i;
				temp2 = 0;
				direction_line -= width_d * 3;
				f = 1;
				break;
			case 3:
				--i;
				temp2 = 2;
				direction_line -= width_d * 3;
				f = 1;
				break;
			case 4:
				--j;
				temp2 = 1;
				f = 2;
				break;
			case 5:
				--j;
				temp2 = 2;
				f = 2;
				break;
			default:
				fprintf(stderr, "Trace back error: %d.\n", direction_line[temp1 - 1]);
				return 0;
		}
		if (f == max) ++e;
		else {
			++l;
			while (l >= s) {
				++s;
				kroundup32(s);
				c = (uint32_t*)realloc(c, s * sizeof(uint32_t));
			}
			c[l - 1] = e<<4|max;
			max = f;
			e = 1;
		}
	}
	if (f == 0) {
		++l;
		while (l >= s) {
			++s;
			kroundup32(s);
			c = (uint32_t*)realloc(c, s * sizeof(uint32_t));
		}
		c[l - 1] = (e+1)<<4;
	}else {
		l += 2;
		while (l >= s) {
			++s;
			kroundup32(s);
			c = (uint32_t*)realloc(c, s * sizeof(uint32_t));
		}
		c[l - 2] = e<<4|f;
		c[l - 1] = 16;
	}

	// reverse cigar
	c1 = (uint32_t*)malloc(l * sizeof(uint32_t));
	s = 0;
	e = l - 1;
	while (LIKELY(s <= e)) {
		c1[s] = c[e];
		c1[e] = c[s];
		++ s;
		-- e;
	}
	result->seq = c1;
	result->length = l;

	free(direction);
	free(h_c);
	free(e_b);
	free(h_b);
	free(c);
	return result;
}

int8_t* seq_reverse(const int8_t* seq, int32_t end) {
	int8_t* reverse = (int8_t*)calloc(end + 1, sizeof(int8_t));
	int32_t start = 0;
	while (LIKELY(start <= end)) {
		reverse[start] = seq[end];
		reverse[end] = seq[start];
		++ start;
		-- end;
	}
	return reverse;
}

/* ---------------- scalar fallback for non-SSE builds ---------------- */
#if !HAVE_SSE2

/* Scalar DP-based Smith-Waterman with affine gaps (Gotoh-style),
   returns best and second best column maxima and their ending positions.
   This is slower but portable. */
alignment_end* sw_scalar (const int8_t* ref,
						  int8_t ref_dir,
						  int32_t refLen,
						  int32_t readLen,
						  int32_t weight_gapO,
						  int32_t weight_gapE,
						  const int8_t* read,
						  const int8_t* mat,
						  int32_t n,
						  int32_t terminate,
						  int32_t maskLen) {
	int32_t i, j;
	int32_t global_max = 0;
	int32_t end_ref = -1;
	int32_t end_read = readLen - 1;

	/* Use int arrays size readLen+1 */
	int32_t *H_prev = (int32_t*)calloc(readLen + 1, sizeof(int32_t));
	int32_t *H_curr = (int32_t*)calloc(readLen + 1, sizeof(int32_t));
	int32_t *E_prev = (int32_t*)calloc(readLen + 1, sizeof(int32_t));
	int32_t *E_curr = (int32_t*)calloc(readLen + 1, sizeof(int32_t));
	int32_t *maxColumn = (int32_t*)calloc(refLen, sizeof(int32_t));
	int32_t *colbuf_for_max = (int32_t*)calloc(readLen + 1, sizeof(int32_t)); /* to store column when we find new global max */

	int step = (ref_dir == 1) ? -1 : 1;
	int start = (ref_dir == 1) ? refLen - 1 : 0;
	int colIdx = 0;
	for (j = start; LIKELY(j >= 0 && j < refLen); j += step, ++colIdx) {
		int refSym = ref[j];
		int F = 0;
		int col_max = 0;
		H_curr[0] = 0;
		E_curr[0] = 0;
		for (i = 1; i <= readLen; ++i) {
			/* E: gap in reference (insertion to read) */
			int e1 = H_prev[i] - weight_gapO;
			int e2 = E_prev[i] - weight_gapE;
			int e = e1 > e2 ? e1 : e2;
			E_curr[i] = e;
			/* F: gap in read (deletion) - depends on current column values */
			int f1 = H_curr[i-1] - weight_gapO;
			int f2 = F - weight_gapE;
			F = f1 > f2 ? f1 : f2;
			/* match/mismatch */
			int s = H_prev[i-1] + mat[refSym * n + read[i-1]];
			int h = s;
			if (e > h) h = e;
			if (F > h) h = F;
			if (h < 0) h = 0;
			H_curr[i] = h;
			if (h > col_max) col_max = h;
		}
		/* store column max */
		maxColumn[colIdx] = col_max;
		if (col_max > global_max) {
			global_max = col_max;
			end_ref = j;
			/* copy current H column to colbuf_for_max to find smallest end_read later */
			memcpy(colbuf_for_max, H_curr, (readLen + 1) * sizeof(int32_t));
		}
		/* early stop if terminate reached */
		if (terminate >= 0 && col_max == terminate) break;
		/* swap prev/curr */
		int32_t *tmp = H_prev; H_prev = H_curr; H_curr = tmp;
		tmp = E_prev; E_prev = E_curr; E_curr = tmp;
	}
	/* find smallest end_read index with value == global_max */
	for (i = 1; i <= readLen; ++i) {
		if (colbuf_for_max[i] == global_max) {
			int t = i - 1;
			if (t < end_read) end_read = t;
		}
	}
	/* prepare results */
	alignment_end* bests = (alignment_end*) calloc(2, sizeof(alignment_end));
	if (global_max < 0) global_max = 0;
	if (global_max > 65535) bests[0].score = 65535;
	else bests[0].score = (uint16_t)global_max;
	bests[0].ref = end_ref;
	bests[0].read = end_read;
	/* second best: find maxColumn outside masked region around end_ref */
	int second_score = 0;
	int mask_left = (end_ref - maskLen) > 0 ? (end_ref - maskLen) : 0;
	int mask_right = (end_ref + maskLen) >= refLen ? refLen - 1 : (end_ref + maskLen);
	for (colIdx = 0; colIdx < refLen; ++colIdx) {
		/* map colIdx back to actual ref index used above */
		int actual_ref_idx = (ref_dir == 1) ? (refLen - 1 - colIdx) : colIdx;
		if (actual_ref_idx >= mask_left && actual_ref_idx <= mask_right) continue;
		if (maxColumn[colIdx] > second_score) {
			second_score = maxColumn[colIdx];
			bests[1].score = (uint16_t)second_score;
			bests[1].ref = actual_ref_idx;
		}
	}
	/* cleanup */
	free(H_prev); free(H_curr); free(E_prev); free(E_curr);
	free(maxColumn); free(colbuf_for_max);
	return bests;
}

#endif /* !HAVE_SSE2 */

/* ---------------- public API: ssw_init / init_destroy / ssw_align / align_destroy ---------------- */

s_profile* ssw_init (const int8_t* read, const int32_t readLen, const int8_t* mat, const int32_t n, const int8_t score_size) {
	s_profile* p = (s_profile*)calloc(1, sizeof(struct _profile));
	p->profile_byte = 0;
	p->profile_word = 0;
	p->bias = 0;
	p->read = read;
	p->mat = mat;
	p->readLen = readLen;
	p->n = n;

#if HAVE_SSE2
	if (score_size == 0 || score_size == 2) {
		int32_t bias = 0, i;
		for (i = 0; i < n*n; i++) if (mat[i] < bias) bias = mat[i];
		bias = abs(bias);
		p->bias = bias;
		p->profile_byte = qP_byte(read, mat, readLen, n, bias);
	}
	if (score_size == 1 || score_size == 2) p->profile_word = qP_word(read, mat, readLen, n);
#else
	/* On non-SSE builds we don't precompute SIMD profiles. Keep profile pointers NULL.
	   The scalar fallback uses mat/read directly during alignment. */
	(void)score_size;
#endif
	return p;
}

void init_destroy (s_profile* p) {
#if HAVE_SSE2
	if (p) {
		free(p->profile_byte);
		free(p->profile_word);
		free(p);
	}
#else
	if (p) free(p);
#endif
}

s_align* ssw_align (const s_profile* prof,
					const int8_t* ref,
				  	int32_t refLen,
				  	const uint8_t weight_gapO,
				  	const uint8_t weight_gapE,
					const uint8_t flag,
					const uint16_t filters,
					const int32_t filterd,
					const int32_t maskLen) {

	alignment_end* bests = NULL;
	alignment_end* bests_reverse = NULL;
#if HAVE_SSE2
    __m128i* vP = NULL;
#else
    void* vP = NULL;
#endif

	int32_t word = 0, band_width = 0, readLen = prof->readLen;
	int8_t* read_reverse = NULL;
	cigar* path = NULL;
	s_align* r = (s_align*)calloc(1, sizeof(s_align));
	r->ref_begin1 = -1;
	r->read_begin1 = -1;
	r->cigar = 0;
	r->cigarLen = 0;

	if (maskLen < 15) {
		/* note: suboptimal info may not be available */
	}

#if HAVE_SSE2
	/* SSE path */
	if (prof->profile_byte) {
		bests = sw_sse2_byte(ref, 0, refLen, readLen, weight_gapO, weight_gapE, prof->profile_byte, -1, prof->bias, maskLen);
		if (prof->profile_word && bests[0].score == 255) {
			free(bests);
			bests = sw_sse2_word(ref, 0, refLen, readLen, weight_gapO, weight_gapE, prof->profile_word, -1, maskLen);
			word = 1;
		} else if (bests[0].score == 255) {
			fprintf(stderr, "Please set 2 to the score_size parameter of the function ssw_init, otherwise the alignment results will be incorrect.\n");
			free(bests);
			free(r);
			return 0;
		}
	} else if (prof->profile_word) {
		bests = sw_sse2_word(ref, 0, refLen, readLen, weight_gapO, weight_gapE, prof->profile_word, -1, maskLen);
		word = 1;
	} else {
		fprintf(stderr, "Please call the function ssw_init before ssw_align.\n");
		free(r);
		return 0;
	}
#else
	/* Scalar fallback path */
	bests = sw_scalar(ref, 0, refLen, readLen, (int)weight_gapO, (int)weight_gapE, prof->read, prof->mat, prof->n, -1, maskLen);
#endif

	r->score1 = bests[0].score;
	r->ref_end1 = bests[0].ref;
	r->read_end1 = bests[0].read;
	if (maskLen >= 15) {
		r->score2 = bests[1].score;
		r->ref_end2 = bests[1].ref;
	} else {
		r->score2 = 0;
		r->ref_end2 = -1;
	}
	free(bests);

	if (flag == 0 || (flag == 2 && r->score1 < filters)) goto end;

	/* Find beginning of best alignment */
	read_reverse = seq_reverse(prof->read, r->read_end1);
#if HAVE_SSE2
	if (!word) {
		vP = qP_byte(read_reverse, prof->mat, r->read_end1 + 1, prof->n, prof->bias);
		bests_reverse = sw_sse2_byte(ref, 1, r->ref_end1 + 1, r->read_end1 + 1, weight_gapO, weight_gapE, vP, r->score1, prof->bias, maskLen);
	} else {
		vP = qP_word(read_reverse, prof->mat, r->read_end1 + 1, prof->n);
		bests_reverse = sw_sse2_word(ref, 1, r->ref_end1 + 1, r->read_end1 + 1, weight_gapO, weight_gapE, vP, r->score1, maskLen);
	}
	free(vP);
#else
	/* Scalar reverse search */
	bests_reverse = sw_scalar(ref, 1, r->ref_end1 + 1, r->read_end1 + 1, (int)weight_gapO, (int)weight_gapE, read_reverse, prof->mat, prof->n, r->score1, maskLen);
#endif
	free(read_reverse);

	if (!bests_reverse) goto end;
	r->ref_begin1 = bests_reverse[0].ref;
	r->read_begin1 = r->read_end1 - bests_reverse[0].read;
	free(bests_reverse);

	if ((7&flag) == 0 || ((2&flag) != 0 && r->score1 < filters) || ((4&flag) != 0 && (r->ref_end1 - r->ref_begin1 > filterd || r->read_end1 - r->read_begin1 > filterd))) goto end;

	/* Generate cigar via banded SW */
	refLen = r->ref_end1 - r->ref_begin1 + 1;
	readLen = r->read_end1 - r->read_begin1 + 1;
	band_width = abs(refLen - readLen) + 1;
	path = banded_sw(ref + r->ref_begin1, prof->read + r->read_begin1, refLen, readLen, r->score1, weight_gapO, weight_gapE, band_width, prof->mat, prof->n);
	if (path == 0) { free(r); r = 0; }
	else {
		r->cigar = path->seq;
		r->cigarLen = path->length;
		free(path);
	}

end:
	return r;
}

void align_destroy (s_align* a) {
	if (!a) return;
	free(a->cigar);
	free(a);
}

