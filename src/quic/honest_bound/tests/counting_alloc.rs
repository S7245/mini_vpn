//! A3 (test-only): process-wide counting allocator for A2 measurement.
//! Live/peak bytes over the System allocator; installed only in the lib test
//! binary, never a runtime build. Peak readings are meaningful only for a
//! measurement run alone (`--exact` + `--test-threads=1`), since all tests in
//! the binary share the counter.
//!
//! Domain attribution (§5.3, D22): every allocation carries a one-byte domain
//! tag in a private header written at allocation time; realloc keeps it and
//! dealloc settles against it, so an allocation stays in its allocating domain
//! for its whole life. `Foreign` = a thread marked by [`mark_thread_foreign`]
//! or code run inside [`foreign`]/[`foreign_sync`]; everything else (all other
//! threads, runtime internals, observers) is conservatively the Node domain.
//! `live`/`peak` remain the untagged whole-process totals.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering::Relaxed},
};

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);
static NODE_LIVE: AtomicUsize = AtomicUsize::new(0);
static NODE_PEAK: AtomicUsize = AtomicUsize::new(0);
const NODE: u8 = 0;
const FOREIGN: u8 = 1;
const MIN_HEADER: usize = 16;

thread_local! {
    static FOREIGN_DEPTH: Cell<usize> = const { Cell::new(0) };
    static TRACING: Cell<bool> = const { Cell::new(false) };
}
/// Node-domain live allocation counts by exact size (< SIZE_CLASSES) or by
/// power-of-two class above it: the §5.3 residual decomposition (D25).
const SIZE_CLASSES: usize = 1 << 17;
static NODE_BY_SIZE: [AtomicUsize; SIZE_CLASSES + 64] = [const { AtomicUsize::new(0) }; SIZE_CLASSES + 64];
static TRACE_SIZES: [AtomicUsize; 8] = [const { AtomicUsize::new(0) }; 8];
static TRACE_LEFT: AtomicUsize = AtomicUsize::new(0);
fn class(size: usize) -> usize {
    if size < SIZE_CLASSES { size } else { SIZE_CLASSES + (usize::BITS - size.leading_zeros()) as usize }
}
fn census(size: usize, add: bool) {
    let slot = &NODE_BY_SIZE[class(size)];
    if add { slot.fetch_add(1, Relaxed); } else { slot.fetch_sub(1, Relaxed); }
}
/// Diagnostic only: first Node-domain allocations of the armed sizes print a
/// backtrace (captured in the Foreign domain, never recursively).
fn trace(size: usize) {
    if TRACE_LEFT.load(Relaxed) == 0 || !TRACE_SIZES.iter().any(|s| { let armed = s.load(Relaxed); armed != 0 && class(armed) == class(size) }) {
        return;
    }
    if TRACING.try_with(|t| t.replace(true)).unwrap_or(true) {
        return;
    }
    if TRACE_LEFT.fetch_update(Relaxed, Relaxed, |n| n.checked_sub(1)).is_ok() {
        foreign_sync(|| eprintln!("[A3-size-trace] size={size}\n{}", std::backtrace::Backtrace::force_capture()));
    }
    TRACING.with(|t| t.set(false));
}

struct Counting;
#[global_allocator]
static ALLOCATOR: Counting = Counting;

fn domain_now() -> u8 {
    // Thread teardown: conservatively Node.
    match FOREIGN_DEPTH.try_with(Cell::get) {
        Ok(depth) if depth > 0 => FOREIGN,
        _ => NODE,
    }
}
fn header(align: usize) -> usize {
    align.max(MIN_HEADER)
}
fn outer(layout: Layout) -> Option<Layout> {
    let h = header(layout.align());
    Layout::from_size_align(layout.size().checked_add(h)?, h).ok()
}
fn grow(bytes: usize, domain: u8) {
    let live = LIVE.fetch_add(bytes, Relaxed) + bytes;
    PEAK.fetch_max(live, Relaxed);
    if domain == NODE {
        let node = NODE_LIVE.fetch_add(bytes, Relaxed) + bytes;
        NODE_PEAK.fetch_max(node, Relaxed);
    }
}
fn node_object(size: usize, domain: u8, add: bool) {
    if domain == NODE {
        census(size, add);
        if add { trace(size); }
    }
}
fn shrink(bytes: usize, domain: u8) {
    LIVE.fetch_sub(bytes, Relaxed);
    if domain == NODE {
        NODE_LIVE.fetch_sub(bytes, Relaxed);
    }
}
/// Writes the domain tag and returns the user pointer.
unsafe fn tag(base: *mut u8, layout: Layout) -> *mut u8 {
    if base.is_null() {
        return base;
    }
    let domain = domain_now();
    unsafe { base.write(domain) };
    grow(layout.size(), domain);
    node_object(layout.size(), domain, true);
    unsafe { base.add(header(layout.align())) }
}

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some(full) = outer(layout) else { return std::ptr::null_mut() };
        unsafe { tag(System.alloc(full), layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let Some(full) = outer(layout) else { return std::ptr::null_mut() };
        unsafe { tag(System.alloc_zeroed(full), layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let h = header(layout.align());
        let base = unsafe { ptr.sub(h) };
        let domain = unsafe { base.read() };
        // `outer` succeeded when this block was allocated.
        let full = unsafe { Layout::from_size_align_unchecked(layout.size() + h, h) };
        unsafe { System.dealloc(base, full) };
        shrink(layout.size(), domain);
        node_object(layout.size(), domain, false);
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        let h = header(old.align());
        let Some(new_full) = size.checked_add(h).filter(|n| Layout::from_size_align(*n, h).is_ok()) else {
            return std::ptr::null_mut();
        };
        let base = unsafe { ptr.sub(h) };
        let domain = unsafe { base.read() };
        let full = unsafe { Layout::from_size_align_unchecked(old.size() + h, h) };
        // The copied header keeps the allocating domain.
        let new = unsafe { System.realloc(base, full, new_full) };
        if new.is_null() {
            return new;
        }
        if size >= old.size() {
            grow(size - old.size(), domain);
        } else {
            shrink(old.size() - size, domain);
        }
        node_object(old.size(), domain, false);
        node_object(size, domain, true);
        unsafe { new.add(h) }
    }
}

/// Current live bytes.
pub(crate) fn live() -> usize {
    LIVE.load(Relaxed)
}

/// Resets the peak to the current live value and returns that baseline.
pub(crate) fn reset_peak() -> usize {
    let live = live();
    PEAK.store(live, Relaxed);
    live
}

/// Peak live bytes since the last `reset_peak`.
pub(crate) fn peak() -> usize {
    PEAK.load(Relaxed)
}

/// Node-domain live bytes (all allocations not made in a Foreign context).
pub(crate) fn node_live() -> usize {
    NODE_LIVE.load(Relaxed)
}

/// Resets the Node-domain peak to the current Node live value; returns it.
pub(crate) fn node_reset_peak() -> usize {
    let live = node_live();
    NODE_PEAK.store(live, Relaxed);
    live
}

/// Node-domain peak since the last `node_reset_peak`.
pub(crate) fn node_peak() -> usize {
    NODE_PEAK.load(Relaxed)
}

/// Node-domain live objects by size class: `(class_size, count)`, non-zero only.
/// Classes ≥ 2^17 report their power-of-two upper bound.
pub(crate) fn node_census() -> Vec<(usize, usize)> {
    foreign_sync(|| {
        NODE_BY_SIZE.iter().enumerate().filter_map(|(i, n)| {
            let count = n.load(Relaxed);
            let size = if i < SIZE_CLASSES { i } else { 1usize << (i - SIZE_CLASSES) };
            (count > 0).then_some((size, count))
        }).collect()
    })
}

/// Diagnostic: arms up to 8 sizes for `budget` total backtraces.
pub(crate) fn arm_size_trace(sizes: &[usize], budget: usize) {
    for (slot, size) in TRACE_SIZES.iter().zip(sizes) {
        slot.store(*size, Relaxed);
    }
    TRACE_LEFT.store(budget, Relaxed);
}

struct ForeignScope;
impl ForeignScope {
    fn enter() -> Self {
        FOREIGN_DEPTH.with(|d| d.set(d.get() + 1));
        Self
    }
}
impl Drop for ForeignScope {
    fn drop(&mut self) {
        FOREIGN_DEPTH.with(|d| d.set(d.get() - 1));
    }
}

/// Marks the calling thread Foreign for its remaining life (peer runtimes).
pub(crate) fn mark_thread_foreign() {
    std::mem::forget(ForeignScope::enter());
}

/// Runs `work` with its allocations attributed to the Foreign domain.
pub(crate) fn foreign_sync<R>(work: impl FnOnce() -> R) -> R {
    let _scope = ForeignScope::enter();
    work()
}

/// Polls `work` (and builds its state) in the Foreign domain. Only futures that
/// poll peer/generator code, never the Service under test, may be wrapped.
pub(crate) fn foreign<F: Future>(work: F) -> impl Future<Output = F::Output> {
    let mut work = foreign_sync(|| Box::pin(work));
    std::future::poll_fn(move |cx| foreign_sync(|| work.as_mut().poll(cx)))
}

#[test]
#[ignore = "process-wide counter: run alone with --exact --test-threads=1"]
fn counts_live_and_peak_of_real_allocations() {
    const MIB: usize = 1 << 20;
    let base = reset_peak();
    let block = vec![1u8; 4 * MIB];
    assert!(live() >= base + 4 * MIB);
    drop(block);
    assert!(peak() >= base + 4 * MIB, "peak {} base {base}", peak());
    let mut grown = Vec::<u8>::with_capacity(MIB);
    grown.reserve_exact(3 * MIB);
    assert!(peak() >= base + 4 * MIB);
    drop(grown);
}

#[test]
#[ignore = "process-wide counter: run alone with --exact --test-threads=1"]
fn foreign_allocations_keep_their_domain_for_life() {
    const MIB: usize = 1 << 20;
    let node = node_reset_peak();
    let total = reset_peak();
    // Foreign alloc + realloc in Node context + free in Node context: never Node.
    let mut block = foreign_sync(|| vec![1u8; 4 * MIB]);
    block.reserve_exact(4 * MIB);
    let aligned = foreign_sync(|| Box::new([0u64; 1 << 17]));
    assert!(live() >= total + 9 * MIB);
    drop((block, aligned));
    assert!(peak() >= total + 9 * MIB);
    assert!(node_peak() < node + MIB, "node peak {} base {node}", node_peak());
    // Node alloc freed inside a Foreign scope still settles against Node.
    let block = vec![2u8; 2 * MIB];
    assert!(node_live() >= node + 2 * MIB);
    foreign_sync(|| drop(block));
    assert!(node_live() < node + MIB);
    // A marked thread is Foreign for life.
    std::thread::spawn(|| {
        mark_thread_foreign();
        drop(vec![3u8; 8 * MIB]);
    })
    .join()
    .unwrap();
    assert!(node_peak() < node + 3 * MIB, "node peak {} base {node}", node_peak());
}
