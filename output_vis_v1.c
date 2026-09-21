/*
 * LampaStream v1 producer for squeezelite's visualiser shared memory.
 *
 * This file is a self-contained reference for the writes squeezelite must
 * perform in output_vis.c so that LampaStream's consumer (see
 * src/lampastream/pcm_source.py: SqueezeliteShmSource / SqueezeliteShmStereoSource)
 * can observe an atomic, monotonic view of the ring buffer.
 *
 * Integration is automatic through scripts/build-squeezelite.sh and the
 * committed output_vis_v1.patch. Initialization uses begin_init BEFORE legacy
 * metadata writes and finish_init AFTER them; normal PCM, silence and stop
 * transitions all use begin_write/end_write. Export-lock failures record gaps.
 * See squeezelite/README.md for layout, units, build and validation boundaries.
 */

#include "vis_shm_v1.h"

#include <errno.h>
#include <fcntl.h>
#include <stdatomic.h>
#include <string.h>
#include <sys/random.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

/*
 * Producer-local pending gap counter.  gap_seq is only *written* to SHM
 * inside a completed write pair — never while the export lock is held by
 * another thread — so we buffer it here between skipped exports.
 */
static uint64_t vis_shm_v1_pending_gaps = 0;

/*
 * Read a uint64 of high-quality randomness for the generation field.
 *
 * Primary source: getrandom(2).  When getrandom() is unavailable (kernel too
 * old, or blocked) fall back to reading /dev/urandom directly.  If both
 * sources fail we return non-zero and the caller MUST abort SHM setup — a
 * PID/time-based fallback can collide across process restarts and would
 * silently defeat the whole reason generation exists.
 */
static int vis_shm_v1_read_generation(uint64_t *out) {
    ssize_t got = getrandom(out, sizeof(*out), 0);
    if (got == (ssize_t)sizeof(*out)) {
        return 0;
    }

    int fd = open("/dev/urandom", O_RDONLY | O_CLOEXEC);
    if (fd < 0) {
        return -1;
    }
    size_t remaining = sizeof(*out);
    unsigned char *buf = (unsigned char *)out;
    while (remaining > 0) {
        ssize_t n = read(fd, buf, remaining);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) {
                continue;
            }
            close(fd);
            return -1;
        }
        buf += n;
        remaining -= (size_t)n;
    }
    close(fd);
    return 0;
}

/*
 * Force write_seq to an ODD value regardless of previous contents.
 *
 * shm_open(O_CREAT | O_RDWR) may reuse an existing SHM segment left over
 * from a previous producer, so the current write_seq is unknown: it may be
 * 0, 1, 2, 3, or any prior value including near-UINT32_MAX.  A naive
 * ``fetch_add(1)`` would happily flip an already-odd value to an even one,
 * publishing an "initialisation in progress" state as "stable snapshot".
 *
 * This helper reads the current value, computes an ODD successor
 * modulo uint32 (odd → +2, even → +1), and stores it back with
 * a seq-cst store + fence so all subsequent writes are ordered after.
 */
void vis_shm_v1_begin_init(vis_shm_v1_ext_t *ext) {
    uint32_t cur = __atomic_load_n(&ext->write_seq, __ATOMIC_SEQ_CST);
    uint32_t next = (cur & 1u) ? (uint32_t)(cur + 2u) : (uint32_t)(cur + 1u);
    __atomic_store_n(&ext->write_seq, next, __ATOMIC_SEQ_CST);
    atomic_thread_fence(memory_order_seq_cst);
    vis_shm_v1_pending_gaps = 0;
}

/*
 * Populate the extension block and flip write_seq to an EVEN value.
 *
 * Called after vis_shm_v1_begin_init() and (optionally) after the caller
 * has published legacy header fields (buf_size, running, rate, buf_index).
 * Returns 0 on success and -1 on RNG failure; on -1 write_seq is LEFT ODD
 * so no reader accepts the segment and the caller must abort SHM setup.
 */
int vis_shm_v1_finish_init(vis_shm_v1_ext_t *ext) {
    uint64_t gen = 0;
    if (vis_shm_v1_read_generation(&gen) != 0) {
        /* Leave write_seq odd so no reader accepts this segment. */
        return -1;
    }

    /*
     * Populate the remaining fields.  Cannot use memset(ext, 0, sizeof(*ext))
     * because that would clobber the odd write_seq marker set by
     * vis_shm_v1_begin_init().
     */
    ext->magic         = VIS_SHM_V1_MAGIC;
    ext->abi_version   = VIS_SHM_V1_VERSION;
    ext->flags         = 0;
    ext->generation    = gen;
    ext->abs_write_pos = 0;
    ext->gap_seq       = 0;
    memset(ext->_pad, 0, sizeof(ext->_pad));

    /* Ensure all field writes are visible before flipping write_seq even. */
    atomic_thread_fence(memory_order_seq_cst);
    uint32_t cur = __atomic_load_n(&ext->write_seq, __ATOMIC_SEQ_CST);
    /* cur is odd (begin_init established that); bump to next even. */
    __atomic_store_n(&ext->write_seq, (uint32_t)(cur + 1u), __ATOMIC_SEQ_CST);
    atomic_thread_fence(memory_order_seq_cst);
    return 0;
}

/*
 * Call once during squeezelite startup, after the SHM segment is mapped.
 *
 * Convenience wrapper for the two-phase init helpers above — used when the
 * caller has no legacy header writes to sequence between begin_init and
 * finish_init.  Returns 0 on success, -1 on RNG failure (caller must abort
 * SHM setup).
 */
int vis_shm_v1_init(vis_shm_v1_ext_t *ext) {
    vis_shm_v1_begin_init(ext);
    return vis_shm_v1_finish_init(ext);
}

/*
 * Seqlock begin: mark write in progress.  After this call, write_seq is odd
 * and readers using the coherent-snapshot protocol will spin/retry until
 * end_write below flips it back to even.
 */
void vis_shm_v1_begin_write(vis_shm_v1_ext_t *ext) {
    __atomic_fetch_add(&ext->write_seq, 1u, __ATOMIC_SEQ_CST);
}

/*
 * Seqlock end: publish the new abs_write_pos and any pending gap increments,
 * then flip write_seq back to even.  n_stereo_frames_written is the number of
 * stereo frames the caller has just written to the ring buffer since the
 * matching begin_write().
 */
void vis_shm_v1_end_write(vis_shm_v1_ext_t *ext, uint64_t n_stereo_frames_written) {
    ext->abs_write_pos += n_stereo_frames_written;
    if (vis_shm_v1_pending_gaps) {
        ext->gap_seq += vis_shm_v1_pending_gaps;
        vis_shm_v1_pending_gaps = 0;
    }
    __atomic_fetch_add(&ext->write_seq, 1u, __ATOMIC_SEQ_CST);
}

/*
 * Record that the current export cycle was skipped because the export lock
 * (trywrlock) could not be acquired.  This defers the SHM update until the
 * next successful vis_shm_v1_begin/end_write pair, so we never touch write_seq
 * while another thread holds the write lock.
 */
void vis_shm_v1_record_gap(vis_shm_v1_ext_t *ext) {
    (void)ext;
    vis_shm_v1_pending_gaps++;
}

/*
 * Convenience: atomically write a stereo block into the ring buffer.
 * Intended as a drop-in for squeezelite's existing export path — the caller
 * is responsible for maintaining buf_index (and handling wrap-around) since
 * the legacy vis_t layout is unchanged.
 *
 *   ring_buffer     – pointer to the int16_t ring at offset 120 in the SHM
 *   ring_capacity   – size of the ring in scalar int16 samples (== buf_size)
 *   ring_write_pos  – pointer to the current scalar-sample write position
 *                     (== buf_index; caller updates *before* end_write so the
 *                     consumer sees them together under the seqlock)
 *   src             – interleaved stereo int16 samples
 *   n_stereo_frames – number of stereo frames in src
 */
void vis_shm_v1_write_samples(
    vis_shm_v1_ext_t *ext,
    int16_t         *ring_buffer,
    size_t           ring_capacity,
    uint32_t        *ring_write_pos,
    const int16_t   *src,
    uint64_t         n_stereo_frames
) {
    if (n_stereo_frames == 0) {
        return;
    }
    vis_shm_v1_begin_write(ext);
    uint32_t pos = *ring_write_pos;
    uint64_t n_scalar = n_stereo_frames * 2u;
    for (uint64_t i = 0; i < n_scalar; i++) {
        ring_buffer[pos] = src[i];
        pos++;
        if (pos >= ring_capacity) {
            pos = 0;
        }
    }
    *ring_write_pos = pos;
    vis_shm_v1_end_write(ext, n_stereo_frames);
}
