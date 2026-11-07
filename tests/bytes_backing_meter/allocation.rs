//! Thread-local observation/failure of the real allocator, never production.
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
};

#[derive(Clone, Copy, Debug, Default)]
pub(super) struct Events {
    pub calls: usize,
    pub allocated: usize,
    pub freed: usize,
}
#[derive(Clone, Copy, Default)]
struct Scope {
    enabled: bool,
    fail_at: Option<usize>,
    events: Events,
}
thread_local! { static SCOPE: Cell<Scope> = Cell::new(Scope::default()); }
struct Observed;
#[global_allocator]
static ALLOCATOR: Observed = Observed;
fn allocation(layout: Layout) -> bool {
    SCOPE
        .try_with(|cell| {
            let mut s = cell.get();
            if !s.enabled {
                return false;
            }
            s.events.calls += 1;
            let fail = s.fail_at == Some(s.events.calls);
            if !fail {
                s.events.allocated += layout.size();
            }
            cell.set(s);
            fail
        })
        .unwrap_or(false)
}
unsafe impl GlobalAlloc for Observed {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if allocation(layout) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc(layout) }
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        if allocation(layout) {
            return std::ptr::null_mut();
        }
        unsafe { System.alloc_zeroed(layout) }
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        let _ = SCOPE.try_with(|cell| {
            let mut s = cell.get();
            if s.enabled {
                s.events.freed += layout.size();
                cell.set(s);
            }
        });
        unsafe { System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, old: Layout, size: usize) -> *mut u8 {
        // No metered constructor/freeze operation is allowed to use realloc.
        let new = unsafe { Layout::from_size_align_unchecked(size, old.align()) };
        if allocation(new) {
            return std::ptr::null_mut();
        }
        unsafe { System.realloc(ptr, old, size) }
    }
}
struct Reset;
impl Drop for Reset {
    fn drop(&mut self) {
        SCOPE.with(|cell| {
            let mut s = cell.get();
            s.enabled = false;
            cell.set(s);
        });
    }
}
pub(super) fn observe<T>(fail_at: Option<usize>, f: impl FnOnce() -> T) -> (T, Events) {
    SCOPE.with(|cell| {
        assert!(!cell.get().enabled);
        cell.set(Scope {
            enabled: true,
            fail_at,
            events: Events::default(),
        });
    });
    let reset = Reset;
    let result = f();
    drop(reset);
    (result, SCOPE.with(|cell| cell.get().events))
}
