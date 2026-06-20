//! Lock-free reads, single-writer skiplist.
//!
//! # Invariants this module relies on
//!
//! 1. **Single writer**: at most one thread calls `&mut self`
//!    methods (`insert`, `insert_with`) at a time. The borrow
//!    checker enforces this at the `&mut self` boundary; the
//!    `unsafe impl Sync` below relies on it.
//!
//! 2. **Arena lifetime**: every `Node` and every key-byte slice
//!    is allocated in the `Bump` arena owned by the `SkipList`.
//!    Pointers (`*mut Node`, `*const [u8]`) are stable for the
//!    SkipList's lifetime. Dropping the SkipList drops the
//!    arena, which invalidates every outstanding pointer; any
//!    `&[u8]` returned by `key()` is borrowed from the SkipList
//!    and tied to its lifetime. `SkipListCursor` carries a
//!    `*const SkipList` and trusts the caller (typically
//!    `MemTable`'s `Arc<UnsafeCell<...>>`) to keep the SkipList
//!    alive for the cursor's lifetime - the type system does
//!    not enforce this.
//!
//! 3. **Level invariant**: a node `N` allocated with height `H`
//!    has a forward-link array of length exactly `H` (stored as
//!    `next: *mut [AtomicPtr<Node>]`). Indexing `next[i]` for
//!    `i >= H` is undefined behavior. Algorithms preserve this
//!    by only following `next[level]` from a node `N` reached
//!    via someone else's `next[level]` link (and the head's
//!    height is `MAX_HEIGHT`).
//!
//! 4. **Memory ordering**: forward-link writes use `Release`,
//!    reads use `Acquire`. This pairs visibility of the new
//!    node's contents with reader threads following the link.
//!    No-barrier variants are only used by the single writer
//!    on its own newly-allocated node before publishing.

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

use crate::comparator::Comparator;

// -- Arena --------------------------------------------------
//
// Fixed-block bump allocator:
// 4 KiB blocks, allocations larger than
// BLOCK_SIZE/4 get their own dedicated block, smaller
// allocations bump-allocate within the current block and
// waste the remainder when a fresh block is needed. We chose
// this over a chunk-doubling arena (e.g. bumpalo) because
// the predictable block layout keeps the skiplist walk
// cache-friendly: every node + its forward-pointer array fits
// alongside its neighbors in the same 4 KiB page.

const BLOCK_SIZE: usize = 4096;

struct Arena {
    alloc_ptr: *mut u8,
    alloc_bytes_remaining: usize,
    blocks: Vec<Box<[u8]>>,
}

// SAFETY: `Arena` owns its blocks via `Box<[u8]>`. The raw
// pointers it stores reference into those owned blocks; moving
// the Arena across threads moves the blocks too. No `Sync` is
// implied or required - all mutation goes through `&mut self`.
unsafe impl Send for Arena {}

impl Arena {
    fn new() -> Self {
        Self {
            alloc_ptr: ptr::null_mut(),
            alloc_bytes_remaining: 0,
            blocks: Vec::new(),
        }
    }

    /// Allocate `bytes` bytes aligned to `align` (must be a
    /// power of two). Returns a pointer into one of the arena's
    /// blocks; the pointer is stable for the arena's lifetime.
    ///
    /// Returns a pointer aligned to `align` bytes.
    fn allocate_aligned(&mut self, bytes: usize, align: usize) -> *mut u8 {
        debug_assert!(align.is_power_of_two());
        let current_mod = (self.alloc_ptr as usize) & (align - 1);
        let slop = if current_mod == 0 {
            0
        } else {
            align - current_mod
        };
        let needed = bytes + slop;
        if needed <= self.alloc_bytes_remaining {
            // SAFETY: `alloc_ptr` points into the current block,
            // and `needed <= alloc_bytes_remaining` keeps the
            // bumped pointer within that block.
            unsafe {
                let result = self.alloc_ptr.add(slop);
                self.alloc_ptr = self.alloc_ptr.add(needed);
                self.alloc_bytes_remaining -= needed;
                result
            }
        } else {
            self.allocate_fallback(bytes)
        }
    }

    #[cold]
    fn allocate_fallback(&mut self, bytes: usize) -> *mut u8 {
        if bytes > BLOCK_SIZE / 4 {
            // Object > 1 KiB: give it its own block to avoid
            // wasting most of a 4 KiB block on a single big alloc.
            self.allocate_new_block(bytes)
        } else {
            // Waste the rest of the current block; allocate a
            // fresh full block and bump within it.
            let new_block = self.allocate_new_block(BLOCK_SIZE);
            // SAFETY: `new_block` points at BLOCK_SIZE bytes we
            // just allocated; `bytes <= BLOCK_SIZE/4 < BLOCK_SIZE`.
            unsafe {
                self.alloc_ptr = new_block.add(bytes);
                self.alloc_bytes_remaining = BLOCK_SIZE - bytes;
            }
            new_block
        }
    }

    #[cold]
    fn allocate_new_block(&mut self, size: usize) -> *mut u8 {
        let v = vec![0; size];
        let mut block = v.into_boxed_slice();
        let ptr = block.as_mut_ptr();
        self.blocks.push(block);
        ptr
    }
}

/// Copy `src` into the arena and return a fat pointer to the
/// copy. Empty inputs return a fat null pointer of length 0.
fn arena_alloc_slice_copy(arena: &mut Arena, src: &[u8]) -> *const [u8] {
    if src.is_empty() {
        return ptr::slice_from_raw_parts(ptr::null::<u8>(), 0);
    }
    let dst = arena.allocate_aligned(src.len(), 1);
    // SAFETY: `dst` points at `src.len()` bytes of arena-owned
    // storage we have exclusive access to until we return.
    unsafe {
        ptr::copy_nonoverlapping(src.as_ptr(), dst, src.len());
    }
    ptr::slice_from_raw_parts(dst as *const u8, src.len())
}

/// Allocate `count` `T`s in the arena, initializing each via
/// `init()`. Returns a fat raw pointer to the slice.
fn arena_alloc_slice_fill_with<T>(
    arena: &mut Arena,
    count: usize,
    mut init: impl FnMut() -> T,
) -> *mut [T] {
    if count == 0 {
        return ptr::slice_from_raw_parts_mut(ptr::null_mut::<T>(), 0);
    }
    let layout = std::alloc::Layout::array::<T>(count)
        .expect("arena_alloc_slice_fill_with: layout overflow");
    let raw = arena.allocate_aligned(layout.size(), layout.align()) as *mut T;
    for i in 0..count {
        // SAFETY: `raw.add(i)` stays within the layout we just
        // requested; we have exclusive access until publication.
        unsafe {
            ptr::write(raw.add(i), init());
        }
    }
    ptr::slice_from_raw_parts_mut(raw, count)
}

/// Allocate space for a `T` in the arena and write `value`
/// into it. Returns a stable raw pointer.
fn arena_alloc<T>(arena: &mut Arena, value: T) -> *mut T {
    let layout = std::alloc::Layout::new::<T>();
    let raw = arena.allocate_aligned(layout.size(), layout.align()) as *mut T;
    // SAFETY: `raw` is properly aligned for `T` and points at
    // `size_of::<T>()` bytes of arena-owned storage we have
    // exclusive access to.
    unsafe {
        ptr::write(raw, value);
    }
    raw
}

pub const MAX_HEIGHT: usize = 12;
pub const BRANCHING: u32 = 4;

/// Variable-height node. Nodes live in the SkipList's
/// bump arena; raw pointers we hold are stable
/// for the lifetime of the SkipList.
pub(crate) struct Node {
    key: *const [u8],
    next: *mut [AtomicPtr<Node>],
}

impl Node {
    /// Returns the node's key bytes.
    ///
    /// # Safety
    ///
    /// The owning `SkipList` (and therefore the `Bump` arena
    /// the key was allocated in) must outlive the returned
    /// reference. Module invariant 2 spells this out.
    unsafe fn key(&self) -> &[u8] {
        &*self.key
    }

    /// Atomic-acquire load of `next[level]`. Used by readers
    /// walking the list concurrently with the writer.
    ///
    /// # Safety
    ///
    /// `level` must be `< N`, where `N` is the node's height
    /// (the length of the `next` slice). Module invariant 3
    /// spells out how callers preserve this.
    unsafe fn next_at(&self, level: usize) -> *mut Node {
        (&*self.next)[level].load(Ordering::Acquire)
    }

    /// Atomic-release store to `next[level]`. Pairs with
    /// `next_at`'s acquire load on reader threads.
    ///
    /// # Safety
    ///
    /// `level` must be `< N`, where `N` is the node's height.
    /// Caller must additionally hold `&mut SkipList` (the
    /// single-writer invariant - module invariant 1).
    unsafe fn set_next(&self, level: usize, ptr: *mut Node) {
        (&*self.next)[level].store(ptr, Ordering::Release);
    }

    /// Relaxed-ordering load. Only used by the single writer
    /// on a newly-allocated node before publishing it via
    /// `set_next` (release) on the predecessor - readers
    /// can't observe these reads.
    ///
    /// # Safety
    ///
    /// `level` must be `< N`. Caller must additionally hold
    /// `&mut SkipList`.
    unsafe fn next_at_no_barrier(&self, level: usize) -> *mut Node {
        (&*self.next)[level].load(Ordering::Relaxed)
    }

    /// Relaxed-ordering store. See `next_at_no_barrier`.
    ///
    /// # Safety
    ///
    /// `level` must be `< N`, where `N` is this node's
    /// height, AND this node must not yet be reachable by any
    /// reader (i.e. it's the freshly-allocated node being
    /// initialized in `insert_key_ptr`).
    unsafe fn set_next_no_barrier(&self, level: usize, ptr: *mut Node) {
        (&*self.next)[level].store(ptr, Ordering::Relaxed);
    }
}

fn allocate_node_in(arena: &mut Arena, key: &[u8], height: usize) -> *mut Node {
    let key_ptr = arena_alloc_slice_copy(arena, key);
    allocate_node_for_key(arena, key_ptr, height)
}

fn allocate_node_for_key(arena: &mut Arena, key: *const [u8], height: usize) -> *mut Node {
    let next = arena_alloc_slice_fill_with(arena, height, || AtomicPtr::new(ptr::null_mut()));
    arena_alloc(arena, Node { key, next })
}

/// Single-writer / multi-reader skiplist. Atomic loads on
/// the forward links allow concurrent readers; writes go
/// through `&mut self` and are not reentrant. Nodes are
/// arena-allocated so `*mut Node` pointers stay valid for
/// the SkipList's lifetime.
pub struct SkipList<C: Comparator> {
    arena: Arena,
    head: *mut Node,
    comparator: C,
    height: AtomicUsize,
    rng_state: Cell<u32>,
}

// SAFETY: `SkipList` contains raw `*mut Node` pointers, which
// suppress the auto Send/Sync derive. Manually re-asserting
// both is sound because:
//
// - The pointers reference `Bump` allocations owned by `self`,
//   so transferring ownership across threads (Send) transfers
//   their backing storage too.
// - Concurrent `&self` access (Sync) is bounded to readers.
//   Module invariant 1 forbids concurrent writers; the borrow
//   checker enforces this via `&mut self` on `insert*`.
//   Reader-vs-reader contention on the AtomicPtrs is handled
//   by Acquire/Release.
// - Reader-vs-writer is also safe: writers publish
//   via `set_next` (Release) after `set_next_no_barrier` on
//   the new node; readers see the publish via `next_at`
//   (Acquire). The new node's contents are visible.
unsafe impl<C: Comparator + Send> Send for SkipList<C> {}
unsafe impl<C: Comparator + Sync> Sync for SkipList<C> {}

impl<C: Comparator> SkipList<C> {
    pub fn new(comparator: C) -> Self {
        let mut arena = Arena::new();
        let head = allocate_node_in(&mut arena, &[], MAX_HEIGHT);
        Self {
            arena,
            head,
            comparator,
            height: AtomicUsize::new(1),
            rng_state: Cell::new(0xdead_beef & 0x7fff_ffff),
        }
    }

    pub fn reserve(&mut self, additional: usize) {
        let _ = additional;
    }

    pub fn insert(&mut self, key: Vec<u8>) {
        let key_ptr = arena_alloc_slice_copy(&mut self.arena, &key);
        self.insert_key_ptr(key_ptr);
    }

    pub fn insert_with<F>(&mut self, len: usize, fill: F)
    where
        F: FnOnce(&mut [u8]),
    {
        // Phase N: skip the zero-fill that an init-style API
        // would do; the `fill` closure is contracted to write
        // every byte. The arena returns uninitialized memory
        // and lets the caller
        // overwrite. Saves ~80 bytes/insert of memset on the
        // memtable hot path.
        let (raw, key_slice): (*const u8, &mut [u8]) = if len == 0 {
            (ptr::null::<u8>(), &mut [])
        } else {
            // Keys are `u8`, so 1-byte alignment is sufficient.
            let raw = self.arena.allocate_aligned(len, 1);
            // SAFETY: `raw` points at `len` bytes of arena-owned
            // storage that we have exclusive access to. The
            // `fill` closure writes every byte before we hand
            // the pointer to `insert_key_ptr`, so no reader ever
            // observes uninitialized memory.
            let slice = unsafe { std::slice::from_raw_parts_mut(raw, len) };
            (raw as *const u8, slice)
        };
        fill(key_slice);
        let key_ptr = ptr::slice_from_raw_parts(raw, len);
        self.insert_key_ptr(key_ptr);
    }

    fn insert_key_ptr(&mut self, key_ptr: *const [u8]) {
        let key = unsafe { &*key_ptr };
        let mut prev: [*mut Node; MAX_HEIGHT] = [ptr::null_mut(); MAX_HEIGHT];
        let existing = self.find_greater_or_equal(key, Some(&mut prev));
        // Phase N: `debug_assert!` is a no-op in release
        // builds. The release-build
        // version of this check would do an extra comparator call
        // per insert - 10k extra calls per write batch at scale.
        debug_assert!(
            existing.is_null()
                || self
                    .comparator
                    .compare(unsafe { (*existing).key() }, key)
                    .is_ne(),
            "SkipList::insert called with duplicate key"
        );

        let height = self.random_height();
        let current_height = self.height.load(Ordering::Acquire);
        if height > current_height {
            for slot in prev.iter_mut().take(height).skip(current_height) {
                *slot = self.head;
            }
            self.height.store(height, Ordering::Release);
        }

        let node_ptr = allocate_node_for_key(&mut self.arena, key_ptr, height);
        unsafe {
            for (i, prev_ptr) in prev.iter().copied().enumerate().take(height) {
                let prev_node = &*prev_ptr;
                (*node_ptr).set_next_no_barrier(i, prev_node.next_at_no_barrier(i));
                prev_node.set_next(i, node_ptr);
            }
        }
    }

    pub fn contains(&self, key: &[u8]) -> bool {
        let n = self.find_greater_or_equal(key, None);
        !n.is_null() && unsafe { self.comparator.compare((*n).key(), key).is_eq() }
    }

    pub fn iter(&self) -> SkipListIter<'_, C> {
        SkipListIter {
            list: self,
            node: ptr::null_mut(),
        }
    }

    pub(crate) fn cursor(&self) -> SkipListCursor<C> {
        SkipListCursor {
            list: self as *const SkipList<C>,
            node: ptr::null_mut(),
            _marker: PhantomData,
        }
    }

    fn find_greater_or_equal(
        &self,
        key: &[u8],
        mut prev: Option<&mut [*mut Node; MAX_HEIGHT]>,
    ) -> *mut Node {
        let mut x = self.head;
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            let next = unsafe { (*x).next_at(level) };
            if !next.is_null()
                && self
                    .comparator
                    .compare(unsafe { (*next).key() }, key)
                    .is_lt()
            {
                x = next;
            } else {
                if let Some(prev) = prev.as_deref_mut() {
                    prev[level] = x;
                }
                if level == 0 {
                    return next;
                }
                level -= 1;
            }
        }
    }

    fn find_less_than(&self, key: &[u8]) -> *mut Node {
        let mut x = self.head;
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            let next = unsafe { (*x).next_at(level) };
            if !next.is_null()
                && self
                    .comparator
                    .compare(unsafe { (*next).key() }, key)
                    .is_lt()
            {
                x = next;
            } else {
                if level == 0 {
                    return x;
                }
                level -= 1;
            }
        }
    }

    fn find_last(&self) -> *mut Node {
        let mut x = self.head;
        let mut level = self.height.load(Ordering::Acquire) - 1;
        loop {
            let next = unsafe { (*x).next_at(level) };
            if next.is_null() {
                if level == 0 {
                    return x;
                }
                level -= 1;
            } else {
                x = next;
            }
        }
    }

    fn random_height(&self) -> usize {
        // Raise the height with probability 1/BRANCHING per level.
        let mut height = 1usize;
        while height < MAX_HEIGHT && self.random_next().is_multiple_of(BRANCHING) {
            height += 1;
        }
        height
    }

    fn random_next(&self) -> u32 {
        // Linear congruential generator: state = state * A mod M.
        const M: u32 = 2_147_483_647;
        const A: u64 = 16_807;
        let product = u64::from(self.rng_state.get()) * A;
        let mut seed = ((product >> 31) + (product & u64::from(M))) as u32;
        if seed > M {
            seed -= M;
        }
        self.rng_state.set(seed);
        seed
    }
}

/// Raw cursor used by owners that keep the SkipList alive by
/// other means (for example an Arc-held MemTable iterator).
pub(crate) struct SkipListCursor<C: Comparator> {
    list: *const SkipList<C>,
    node: *mut Node,
    _marker: PhantomData<C>,
}

impl<C: Comparator> SkipListCursor<C> {
    fn list(&self) -> &SkipList<C> {
        unsafe { &*self.list }
    }
    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }
    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.node).key() }
    }
    pub fn seek_to_first(&mut self) {
        self.node = unsafe { (*self.list().head).next_at(0) };
    }
    pub fn seek_to_last(&mut self) {
        let list = self.list();
        let last = list.find_last();
        self.node = if ptr::eq(last, list.head) {
            ptr::null_mut()
        } else {
            last
        };
    }
    pub fn seek(&mut self, target: &[u8]) {
        self.node = self.list().find_greater_or_equal(target, None);
    }
    pub fn next(&mut self) {
        assert!(self.valid());
        self.node = unsafe { (*self.node).next_at(0) };
    }
    pub fn prev(&mut self) {
        assert!(self.valid());
        let key = unsafe { (*self.node).key().to_vec() };
        let list = self.list();
        let candidate = list.find_less_than(&key);
        self.node = if ptr::eq(candidate, list.head) {
            ptr::null_mut()
        } else {
            candidate
        };
    }
}

pub struct SkipListIter<'a, C: Comparator> {
    list: &'a SkipList<C>,
    node: *mut Node,
}

impl<'a, C: Comparator> SkipListIter<'a, C> {
    pub fn valid(&self) -> bool {
        !self.node.is_null()
    }
    pub fn key(&self) -> &[u8] {
        assert!(self.valid());
        unsafe { (*self.node).key() }
    }
    pub fn seek_to_first(&mut self) {
        self.node = unsafe { (*self.list.head).next_at(0) };
    }
    pub fn seek_to_last(&mut self) {
        let last = self.list.find_last();
        self.node = if ptr::eq(last, self.list.head) {
            ptr::null_mut()
        } else {
            last
        };
    }
    pub fn seek(&mut self, target: &[u8]) {
        self.node = self.list.find_greater_or_equal(target, None);
    }
    pub fn next(&mut self) {
        assert!(self.valid());
        self.node = unsafe { (*self.node).next_at(0) };
    }
    pub fn prev(&mut self) {
        assert!(self.valid());
        let key = unsafe { (*self.node).key().to_vec() };
        let candidate = self.list.find_less_than(&key);
        self.node = if ptr::eq(candidate, self.list.head) {
            ptr::null_mut()
        } else {
            candidate
        };
    }
}
