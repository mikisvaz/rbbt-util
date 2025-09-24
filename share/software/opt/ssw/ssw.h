#ifndef SSW_H
#define SSW_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque profile type */
struct _profile;
typedef struct _profile s_profile;

/* Alignment result */
typedef struct {
    uint16_t score1;
    uint16_t score2;
    int32_t  ref_begin1;   /* -1 if not available */
    int32_t  ref_end1;
    int32_t  read_begin1;  /* -1 if not available */
    int32_t  read_end1;
    int32_t  ref_end2;
    uint32_t *cigar;       /* CIGAR ops in BAM-style (len<<4 | type) */
    int32_t  cigarLen;
} s_align;

/* Public API */
s_profile* ssw_init(const int8_t* read, int32_t readLen,
                     const int8_t* mat, int32_t n, int8_t score_size);

void init_destroy(s_profile* p);

s_align* ssw_align(const s_profile* prof,
                   const int8_t* ref, int32_t refLen,
                   uint8_t weight_gapO, uint8_t weight_gapE,
                   uint8_t flag, uint16_t filters, int32_t filterd,
                   int32_t maskLen);

void align_destroy(s_align* a);

#ifdef __cplusplus
}
#endif

#endif /* SSW_H */

