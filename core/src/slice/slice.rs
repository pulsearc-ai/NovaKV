use std::cmp::Ordering;
use std::ops::Index;

/// Borrowed view over a byte range with familiar method
/// names - `data`, `size`, `empty`, `starts_with`, `compare`,
/// `remove_prefix`. v1 hot paths use `&[u8]` directly; this
/// type is for readability and the user-facing API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Slice<'a> {
    data: &'a [u8],
}

impl<'a> Slice<'a> {
    /// Construct a slice over the given byte range.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Returns the underlying byte range.
    pub fn data(&self) -> &'a [u8] {
        self.data
    }

    /// Returns the number of bytes in the slice.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// True if the slice contains no bytes.
    pub fn empty(&self) -> bool {
        self.data.is_empty()
    }

    /// True if the slice begins with `prefix`.
    pub fn starts_with(&self, prefix: Slice<'_>) -> bool {
        self.data.starts_with(prefix.data)
    }

    /// Lexicographic comparison against `other`, exposed as
    /// `Ordering` for Rust ergonomics.
    pub fn compare(&self, other: Slice<'_>) -> Ordering {
        self.data.cmp(other.data)
    }

    /// Copies the slice into an owned byte vector.
    pub fn to_vec(&self) -> Vec<u8> {
        self.data.to_vec()
    }

    /// Advances the slice's start past the first `n` bytes.
    ///
    /// Panics if `n > self.size()`.
    pub fn remove_prefix(&mut self, n: usize) {
        assert!(n <= self.data.len(), "Slice::remove_prefix: out of bounds");
        self.data = &self.data[n..];
    }

    /// Resets the slice to empty.
    pub fn clear(&mut self) {
        self.data = &[];
    }
}

impl<'a> From<&'a [u8]> for Slice<'a> {
    fn from(data: &'a [u8]) -> Self {
        Self::new(data)
    }
}

impl<'a> From<&'a Vec<u8>> for Slice<'a> {
    fn from(data: &'a Vec<u8>) -> Self {
        Self::new(data.as_slice())
    }
}

impl<'a> From<&'a str> for Slice<'a> {
    fn from(data: &'a str) -> Self {
        Self::new(data.as_bytes())
    }
}

impl<'a> Index<usize> for Slice<'a> {
    type Output = u8;
    fn index(&self, index: usize) -> &u8 {
        &self.data[index]
    }
}

impl<'a> AsRef<[u8]> for Slice<'a> {
    fn as_ref(&self) -> &[u8] {
        self.data
    }
}
