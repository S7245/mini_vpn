//! One allocation owns a copied payload, its reference count and Node charge.
//! This is used only when an ordered transport batch already needs a copy.
//! It does not change the ordinary `Bytes` receive path.

use super::{AllocationIdentity, AtomicPtr, Bytes, BytesMut, Vtable};
use crate::BackingMeter;
use crate::backing_meter::BackingMeterInner;
use crate::loom::sync::atomic::{AtomicMut, AtomicUsize, Ordering};
use alloc::alloc::{Layout, alloc, dealloc};
use alloc::sync::Arc;
use alloc::vec::Vec;
use core::{ptr, slice};

#[repr(C)]
struct Header {
    references: AtomicUsize,
    escaped: AtomicUsize,
    meter: Arc<BackingMeterInner>,
    payload_len: usize,
}

fn layout(len: usize) -> Option<Layout> {
    Layout::new::<Header>()
        .extend(Layout::array::<u8>(len).ok()?)
        .ok()
        .map(|(layout, _)| layout.pad_to_align())
}

unsafe fn payload(header: *mut Header) -> *mut u8 {
    unsafe { header.cast::<u8>().add(core::mem::size_of::<Header>()) }
}

pub(super) fn copy_slices(chunks: &[Bytes], meter: &BackingMeter) -> Option<Bytes> {
    for chunk in chunks {
        let actual = chunk.backing_meter();
        if !actual
            .as_ref()
            .is_some_and(|actual| meter.same_authority(actual))
        {
            meter.quarantine();
            if let Some(actual) = actual {
                actual.quarantine();
            }
            return None;
        }
    }
    let len = chunks
        .iter()
        .try_fold(0usize, |n, chunk| n.checked_add(chunk.len()));
    let Some(len) = len.filter(|len| *len != 0) else {
        meter.quarantine();
        return None;
    };
    copy_payload(len, meter, |data| unsafe {
        let mut at = 0;
        for chunk in chunks {
            ptr::copy_nonoverlapping(chunk.as_ptr(), data.add(at), chunk.len());
            at += chunk.len();
        }
    })
}

pub(super) fn copy_from_slice(data: &[u8], meter: &BackingMeter) -> Option<Bytes> {
    if data.is_empty() {
        meter.quarantine();
        return None;
    }
    copy_payload(data.len(), meter, |target| unsafe {
        ptr::copy_nonoverlapping(data.as_ptr(), target, data.len());
    })
}

fn copy_payload(len: usize, meter: &BackingMeter, write: impl FnOnce(*mut u8)) -> Option<Bytes> {
    let Some(layout) = layout(len) else {
        meter.quarantine();
        return None;
    };
    // The one-block layout is reserved before allocation; copies add no block.
    let reservation = meter.inner.reserve_layout(layout.size()).ok()?;
    let raw = unsafe { alloc(layout) };
    if raw.is_null() {
        return None;
    }
    let header = raw.cast::<Header>();
    unsafe {
        ptr::write(
            header,
            Header {
                references: AtomicUsize::new(1),
                escaped: AtomicUsize::new(0),
                meter: meter.inner.clone(),
                payload_len: len,
            },
        );
        let data = payload(header);
        write(data);
        if !meter
            .inner
            .register_preallocated(len, layout.size(), reservation)
        {
            ptr::drop_in_place(header);
            dealloc(raw, layout);
            return None;
        }
        Some(Bytes {
            ptr: data,
            len,
            data: AtomicPtr::new(header.cast()),
            vtable: &VTABLE,
        })
    }
}

pub(super) static VTABLE: Vtable = Vtable {
    clone: clone,
    into_vec: into_vec,
    into_mut: into_mut,
    is_unique: is_unique,
    drop: drop,
};

pub(super) fn backing_meter(data: &AtomicPtr<()>) -> BackingMeter {
    let header = data.load(Ordering::Acquire).cast::<Header>();
    // SAFETY: The caller holds a live Bytes handle to this allocation.
    unsafe {
        BackingMeter {
            inner: (*header).meter.clone(),
        }
    }
}

pub(super) fn allocation_identity(data: &AtomicPtr<()>) -> AllocationIdentity {
    let header = data.load(Ordering::Acquire).cast::<Header>();
    // SAFETY: The caller holds a live Bytes handle to this allocation.
    unsafe {
        AllocationIdentity {
            base: payload(header) as usize,
            capacity: (*header).payload_len,
        }
    }
}

unsafe fn release(header: *mut Header) {
    let old = unsafe { (*header).references.fetch_sub(1, Ordering::Release) };
    if old != 1 {
        if old == 0 {
            unsafe { (*header).meter.quarantine() };
        }
        return;
    }
    unsafe { (*header).references.load(Ordering::Acquire) };
    let len = unsafe { (*header).payload_len };
    let escaped = unsafe { (*header).escaped.load(Ordering::Acquire) != 0 };
    unsafe {
        let Some(layout) = layout(len) else {
            (*header).meter.quarantine();
            crate::abort();
        };
        let meter = (*header).meter.clone();
        ptr::drop_in_place(header);
        dealloc(header.cast(), layout);
        if !escaped {
            meter.release(len, layout.size());
        }
    }
}

unsafe fn clone(data: &AtomicPtr<()>, ptr: *const u8, len: usize) -> Bytes {
    let header = data.load(Ordering::Relaxed).cast::<Header>();
    let old = unsafe { (*header).references.fetch_add(1, Ordering::Relaxed) };
    if old > usize::MAX >> 1 {
        crate::abort();
    }
    Bytes {
        ptr,
        len,
        data: AtomicPtr::new(header.cast()),
        vtable: &VTABLE,
    }
}

unsafe fn into_vec(data: &AtomicPtr<()>, ptr: *const u8, len: usize) -> Vec<u8> {
    let header = data.load(Ordering::Relaxed).cast::<Header>();
    // The returned Vec has no Node charge; keep that uncertainty sticky.
    unsafe { (*header).escaped.store(1, Ordering::Release) };
    unsafe { (*header).meter.quarantine() };
    let copy = Vec::from(unsafe { slice::from_raw_parts(ptr, len) });
    unsafe { release(header) };
    copy
}

unsafe fn into_mut(data: &AtomicPtr<()>, ptr: *const u8, len: usize) -> BytesMut {
    let header = data.load(Ordering::Relaxed).cast::<Header>();
    unsafe { (*header).escaped.store(1, Ordering::Release) };
    unsafe { (*header).meter.quarantine() };
    let copy = BytesMut::from(unsafe { slice::from_raw_parts(ptr, len) });
    unsafe { release(header) };
    copy
}

unsafe fn is_unique(data: &AtomicPtr<()>) -> bool {
    let _ = data;
    false
}

unsafe fn drop(data: &mut AtomicPtr<()>, _ptr: *const u8, _len: usize) {
    data.with_mut(|header| unsafe { release((*header).cast()) });
}
