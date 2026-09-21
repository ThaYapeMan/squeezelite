/*
 * LampaStream v1 SHM ABI — shared between squeezelite producer and Python consumer.
 *
 * This header defines the extension block appended immediately after the legacy
 * squeezelite vis_t ``buffer`` array (at offset 32848 = 80 + sizeof(buffer)),
 * i.e. at the very end of the mmap region rather than in the middle of it.
 * This keeps every field a stock/unmodified consumer reads (rwlock, buf_size,
 * buf_index, running, rate, updated, buffer) at the exact same offset as in
 * unpatched upstream squeezelite, so a single build serves both that consumer
 * and LampaStream's own v1 reader -- see the comment on ``struct vis_t`` in
 * output_vis.c for why this is safe.
 *
 * Layout (40 bytes):
 *   offset  0: uint32_t magic         = VIS_SHM_V1_MAGIC (0x48555345 'HUSE')
 *   offset  4: uint16_t abi_version   = 1
 *   offset  6: uint16_t flags         = 0 (reserved)
 *   offset  8: uint32_t write_seq     = seqlock counter (even=stable, odd=writing)
 *   offset 12: uint64_t generation    = producer lifetime ID (random, changes on restart)
 *   offset 20: uint64_t abs_write_pos = exclusive next stereo-frame position (monotonic)
 *   offset 28: uint64_t gap_seq       = monotonic gap sequence (increments on skipped export)
 *   offset 36: uint8_t  _pad[4]       = 0
 *   Total: 40 bytes
 *
 * All multi-byte fields are little-endian.
 *
 * abs_write_pos unit: stereo frames (one stereo frame = 2* int16_t = 4 bytes).
 * It is the exclusive next write position — after writing N frames, it equals
 * the count of all frames ever written by this producer instance.
 *
 * Seqlock protocol:
 *   Writer: atomic increment write_seq (odd), update all fields, atomic increment write_seq (even)
 *   Reader: read write_seq1; reject if odd; read fields; read write_seq2; accept only if seq1==seq2
 *
 * Generation: initialised from arc4random_buf() or getrandom() on startup.
 * A restarted squeezelite process has a different generation with overwhelming probability.
 */

#pragma once
#include <stdint.h>
#include <stddef.h>

/* Magic value stored little-endian; bytes in memory are 0x45, 0x53, 0x55, 0x48
 * ('E', 'S', 'U', 'H').  Interpreted as a 32-bit little-endian integer this
 * yields 0x48555345, whose ASCII spelling in most-significant-byte-first order
 * is 'HUSE'.  Both forms refer to the same constant. */
#define VIS_SHM_V1_MAGIC     UINT32_C(0x48555345)  /* 'HUSE' */
#define VIS_SHM_V1_VERSION   UINT16_C(1)

/* The extension block begins immediately after the legacy vis_t ``buffer``
 * array -- i.e. at the very end of the mmap region, not between the header
 * and the buffer. */
#define VIS_SHM_V1_EXT_OFFSET 32848U /* = 80 (legacy header: rwlock + buf_size + buf_index +
                                         running + rate + updated, on Linux x86_64) +
                                         16384 * sizeof(int16_t) (buffer) */

typedef struct __attribute__((packed)) vis_shm_v1_ext {
    uint32_t magic;           /* VIS_SHM_V1_MAGIC */
    uint16_t abi_version;     /* VIS_SHM_V1_VERSION */
    uint16_t flags;           /* reserved, write 0 */
    uint32_t write_seq;       /* seqlock (even=stable, odd=writing) */
    uint64_t generation;      /* random uint64 per squeezelite process */
    uint64_t abs_write_pos;   /* exclusive next-stereo-frame position */
    uint64_t gap_seq;         /* skipped-export counter */
    uint8_t  _pad[4];
} vis_shm_v1_ext_t;

_Static_assert(sizeof(vis_shm_v1_ext_t) == 40, "vis_shm_v1_ext_t must be 40 bytes");
_Static_assert(offsetof(vis_shm_v1_ext_t, magic)         ==  0, "magic offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, abi_version)   ==  4, "abi_version offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, flags)         ==  6, "flags offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, write_seq)     ==  8, "write_seq offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, generation)    == 12, "generation offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, abs_write_pos) == 20, "abs_write_pos offset");
_Static_assert(offsetof(vis_shm_v1_ext_t, gap_seq)       == 28, "gap_seq offset");

/*
 * Producer helpers — implemented in output_vis_v1.c.  These live inside
 * squeezelite once the patched output_vis.c is built.
 *
 * vis_shm_v1_init() returns 0 on success and -1 on failure (kernel RNG and
 * /dev/urandom both unavailable).  On failure the caller MUST abort SHM setup;
 * silently falling back to a PID/time-based generation defeats restart
 * detection on the consumer side.
 */
int  vis_shm_v1_init(vis_shm_v1_ext_t *ext);
/*
 * Two-phase init helpers.  The caller uses these when legacy header fields
 * (buf_size, running, rate, buf_index) must be published inside the same
 * seqlock-odd window as the extension block, so a racing consumer never
 * observes a partially-populated snapshot with a stable (even) write_seq.
 *
 * vis_shm_v1_begin_init(ext):
 *   Force write_seq to an ODD value regardless of previous contents.
 *   Handles SHM segments reused with prior write_seq values 0/1/2/3/…
 *   (shm_open O_CREAT | O_RDWR may reuse an existing segment, so the
 *   producer cannot assume mmap zeroes it).  The caller then publishes
 *   whatever legacy metadata it needs before calling finish_init.
 *
 * vis_shm_v1_finish_init(ext):
 *   Populate the extension block (magic, abi_version, generation, etc.)
 *   and flip write_seq to an EVEN value.  Returns 0 on success and -1
 *   on RNG failure — on -1 write_seq is LEFT ODD so readers reject
 *   the segment and the caller must abort SHM setup.
 */
void vis_shm_v1_begin_init(vis_shm_v1_ext_t *ext);
int  vis_shm_v1_finish_init(vis_shm_v1_ext_t *ext);
void vis_shm_v1_begin_write(vis_shm_v1_ext_t *ext);
void vis_shm_v1_end_write(vis_shm_v1_ext_t *ext, uint64_t n_stereo_frames_written);
void vis_shm_v1_record_gap(vis_shm_v1_ext_t *ext);
void vis_shm_v1_write_samples(
    vis_shm_v1_ext_t *ext,
    int16_t         *ring_buffer,
    size_t           ring_capacity,
    uint32_t        *ring_write_pos,
    const int16_t   *src,
    uint64_t         n_stereo_frames
);
