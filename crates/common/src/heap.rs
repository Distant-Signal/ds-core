//! Keeps a worker's short-lived allocation peaks from becoming its pod's
//! resident size.
//!
//! The service binaries use the system allocator: glibc malloc in the
//! images (`debian:bookworm-slim`). A batch step or a publish allocates a
//! lot at once and frees it a moment later, and glibc keeps freed memory
//! by default:
//!
//! - Small chunks go back to the arena's free lists. Only the very top of
//!   the main arena is ever returned on its own, and threads other than
//!   the main one allocate from their own arenas, so a step's freed heap
//!   stays resident. [`release_freed_memory`] runs `malloc_trim(0)`, which
//!   returns every arena's free pages, holes included.
//! - A chunk over the mmap threshold (128 KiB) is its own mapping, unmapped
//!   on free. But glibc raises that threshold to the size of every such
//!   chunk freed (up to 32 MiB), after which multi-MB buffers come from an
//!   arena instead, often at the top of a heap that `malloc_trim` does not
//!   shrink. [`pin_mmap_threshold`] sets the threshold explicitly at
//!   startup, which turns that adjustment off.
//!
//! Measured with both, each alone and neither:
//!
//! - enricher, 2026-10-14 (2,000-incident primary and adversarial batches
//!   against an out-of-process mock): anonymous RSS returned to its ~5 MiB
//!   baseline after each step with both, and stayed at 12-50 MiB with
//!   either alone (168 MiB with neither).
//! - schedule-reference, 2026-10-14: see that crate's
//!   `publish_memory_harness` test for the whole-network publish numbers.
//!
//! Both are no-ops off glibc. [`resident_set_bytes`] and
//! [`peak_resident_set_bytes`] read the kernel's own numbers, for a log line
//! or gauge that shows the release worked.

/// The mmap threshold [`pin_mmap_threshold`] pins: glibc's own default.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
const MMAP_THRESHOLD: libc::c_int = 128 * 1024;

/// Pins glibc's mmap threshold at its default, so allocations over it are
/// always their own mapping, returned to the kernel when freed. Call once,
/// at startup, before the big work. (An explicit `MALLOC_MMAP_THRESHOLD_` in
/// the environment does the same; this makes it the binary's own
/// behaviour.)
pub fn pin_mmap_threshold() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: `mallopt` takes two integers and changes a glibc malloc
        // parameter under its own lock; any thread may call it at any time.
        #[expect(unsafe_code, reason = "an FFI call into glibc; see SAFETY above")]
        let ok = unsafe { libc::mallopt(libc::M_MMAP_THRESHOLD, MMAP_THRESHOLD) };
        if ok != 1 {
            tracing::warn!("could not pin the malloc mmap threshold");
        }
    }
}

/// Hands the heap a step just freed back to the kernel (see the module
/// doc). Call it once the step's big structures are dropped. It walks every
/// arena (milliseconds for a heap of a few GiB), so call it per step, not
/// per item.
pub fn release_freed_memory() {
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: `malloc_trim` takes no pointers and only walks glibc's
        // own arenas under their locks; any thread may call it at any time.
        #[expect(unsafe_code, reason = "an FFI call into glibc; see SAFETY above")]
        let released = unsafe { libc::malloc_trim(0) };
        tracing::debug!(released, "returned freed heap memory to the kernel");
    }
}

/// This process's resident set size (`VmRSS`), in bytes. `None` off Linux
/// or if `/proc/self/status` cannot be read.
#[must_use]
pub fn resident_set_bytes() -> Option<u64> {
    proc_status_bytes("VmRSS:")
}

/// This process's peak resident set size so far (`VmHWM`), in bytes.
/// `None` off Linux or if `/proc/self/status` cannot be read.
#[must_use]
pub fn peak_resident_set_bytes() -> Option<u64> {
    proc_status_bytes("VmHWM:")
}

fn proc_status_bytes(field: &str) -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_status_kib(&status, field).map(|kib| kib * 1024)
}

/// The `kB` value of `field` (e.g. `VmRSS:`) in a `/proc/<pid>/status` text.
fn parse_status_kib(status: &str, field: &str) -> Option<u64> {
    status
        .lines()
        .find_map(|line| line.strip_prefix(field))?
        .trim()
        .strip_suffix("kB")?
        .trim()
        .parse()
        .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_kib_fields_of_proc_status() {
        let status = "Name:\tx\nVmHWM:\t  204800 kB\nVmRSS:\t    8192 kB\nThreads:\t4\n";
        assert_eq!(parse_status_kib(status, "VmRSS:"), Some(8192));
        assert_eq!(parse_status_kib(status, "VmHWM:"), Some(204_800));
        assert_eq!(parse_status_kib(status, "VmSwap:"), None);
        assert_eq!(parse_status_kib(status, "Threads:"), None, "not a kB field");
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn reads_this_process_rss() {
        let rss = resident_set_bytes().expect("Linux has /proc/self/status");
        let peak = peak_resident_set_bytes().expect("Linux has /proc/self/status");
        assert!(rss > 0 && peak >= rss, "rss {rss}, peak {peak}");
    }

    #[test]
    fn the_glibc_calls_do_not_panic() {
        pin_mmap_threshold();
        release_freed_memory();
    }
}
