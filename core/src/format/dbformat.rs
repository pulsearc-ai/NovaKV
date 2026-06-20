use std::cmp::Ordering;

use crate::coding::{decode_fixed64, put_fixed64, put_varint32};
use crate::comparator::{BytewiseComparator, Comparator};

pub const NUM_LEVELS: usize = 7;
pub const L0_COMPACTION_TRIGGER: usize = 4;
pub const L0_SLOWDOWN_WRITES_TRIGGER: usize = 8;
pub const L0_STOP_WRITES_TRIGGER: usize = 12;
pub const MAX_MEM_COMPACT_LEVEL: usize = 2;
pub const READ_BYTES_PERIOD: usize = 1048576;

pub type SequenceNumber = u64;
pub const MAX_SEQUENCE_NUMBER: SequenceNumber = (1u64 << 56) - 1;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[repr(u8)]
pub enum ValueType {
    Deletion = 0x0,
    Value = 0x1,
}

pub const VALUE_TYPE_FOR_SEEK: ValueType = ValueType::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedInternalKey {
    pub user_key: Vec<u8>,
    pub sequence: SequenceNumber,
    pub value_type: ValueType,
}

impl ParsedInternalKey {
    pub fn new(
        user_key: impl AsRef<[u8]>,
        sequence: SequenceNumber,
        value_type: ValueType,
    ) -> Self {
        Self {
            user_key: user_key.as_ref().to_vec(),
            sequence,
            value_type,
        }
    }

    pub fn encoding_len(&self) -> usize {
        self.user_key.len() + 8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InternalKey {
    rep: Vec<u8>,
}

impl InternalKey {
    pub fn new(
        user_key: impl AsRef<[u8]>,
        sequence: SequenceNumber,
        value_type: ValueType,
    ) -> Self {
        let mut rep = Vec::new();
        append_internal_key(
            &mut rep,
            &ParsedInternalKey::new(user_key, sequence, value_type),
        );
        Self { rep }
    }

    pub fn decode_from(&mut self, encoded: impl AsRef<[u8]>) {
        self.rep.clear();
        self.rep.extend_from_slice(encoded.as_ref());
    }

    pub fn encode(&self) -> &[u8] {
        assert!(!self.rep.is_empty(), "empty InternalKey is invalid");
        &self.rep
    }

    pub fn user_key(&self) -> &[u8] {
        extract_user_key(&self.rep).expect("internal key requires an 8-byte tag")
    }

    pub fn clear(&mut self) {
        self.rep.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.rep.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct InternalKeyComparator<C = BytewiseComparator> {
    user_comparator: C,
}

impl Default for InternalKeyComparator<BytewiseComparator> {
    fn default() -> Self {
        Self::new(BytewiseComparator)
    }
}

impl<C: Comparator> InternalKeyComparator<C> {
    pub fn new(user_comparator: C) -> Self {
        Self { user_comparator }
    }

    pub fn user_comparator(&self) -> &C {
        &self.user_comparator
    }

    pub fn name_inherent(&self) -> &'static str {
        "novakv.InternalKeyComparator"
    }

    pub fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        let user_order = self
            .user_comparator
            .compare(required_user_key(a), required_user_key(b));
        if !user_order.is_eq() {
            return user_order;
        }
        let anum = decode_fixed64(&a[a.len() - 8..]);
        let bnum = decode_fixed64(&b[b.len() - 8..]);
        bnum.cmp(&anum)
    }

    pub fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        let user_start = required_user_key(start).to_vec();
        let user_limit = required_user_key(limit);
        let mut tmp = user_start.clone();
        self.user_comparator
            .find_shortest_separator(&mut tmp, user_limit);
        if tmp.len() < user_start.len() && self.user_comparator.compare(&user_start, &tmp).is_lt() {
            put_fixed64(
                &mut tmp,
                pack_sequence_and_type(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
            );
            debug_assert!(self.compare(start, &tmp).is_lt());
            debug_assert!(self.compare(&tmp, limit).is_lt());
            *start = tmp;
        }
    }

    pub fn find_short_successor(&self, key: &mut Vec<u8>) {
        let user_key = required_user_key(key).to_vec();
        let mut tmp = user_key.clone();
        self.user_comparator.find_short_successor(&mut tmp);
        if tmp.len() < user_key.len() && self.user_comparator.compare(&user_key, &tmp).is_lt() {
            put_fixed64(
                &mut tmp,
                pack_sequence_and_type(MAX_SEQUENCE_NUMBER, VALUE_TYPE_FOR_SEEK),
            );
            debug_assert!(self.compare(key, &tmp).is_lt());
            *key = tmp;
        }
    }
}

impl<C: Comparator> Comparator for InternalKeyComparator<C> {
    fn name(&self) -> &'static str {
        self.name_inherent()
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        InternalKeyComparator::compare(self, a, b)
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        InternalKeyComparator::find_shortest_separator(self, start, limit)
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        InternalKeyComparator::find_short_successor(self, key)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LookupKey {
    rep: Vec<u8>,
    kstart: usize,
}

impl LookupKey {
    pub fn new(user_key: impl AsRef<[u8]>, sequence: SequenceNumber) -> Self {
        let user_key = user_key.as_ref();
        let mut rep = Vec::with_capacity(user_key.len() + 13);
        put_varint32(&mut rep, (user_key.len() + 8) as u32);
        let kstart = rep.len();
        rep.extend_from_slice(user_key);
        put_fixed64(
            &mut rep,
            pack_sequence_and_type(sequence, VALUE_TYPE_FOR_SEEK),
        );
        Self { rep, kstart }
    }

    pub fn memtable_key(&self) -> &[u8] {
        &self.rep
    }

    pub fn internal_key(&self) -> &[u8] {
        &self.rep[self.kstart..]
    }

    pub fn user_key(&self) -> &[u8] {
        &self.rep[self.kstart..self.rep.len() - 8]
    }
}

pub fn internal_key_encoding_len(key: &ParsedInternalKey) -> usize {
    key.encoding_len()
}

pub fn append_internal_key(dst: &mut Vec<u8>, key: &ParsedInternalKey) {
    dst.extend_from_slice(&key.user_key);
    put_fixed64(dst, pack_sequence_and_type(key.sequence, key.value_type));
}

pub fn parse_internal_key(internal_key: &[u8]) -> Option<ParsedInternalKey> {
    if internal_key.len() < 8 {
        return None;
    }
    let tag = decode_fixed64(&internal_key[internal_key.len() - 8..]);
    let value_type = value_type_from_tag((tag & 0xff) as u8)?;
    Some(ParsedInternalKey {
        user_key: internal_key[..internal_key.len() - 8].to_vec(),
        sequence: tag >> 8,
        value_type,
    })
}

pub fn extract_user_key(internal_key: &[u8]) -> Option<&[u8]> {
    if internal_key.len() < 8 {
        return None;
    }
    Some(&internal_key[..internal_key.len() - 8])
}

pub fn extract_value_type(internal_key: &[u8]) -> Option<ValueType> {
    if internal_key.len() < 8 {
        return None;
    }
    let tag = decode_fixed64(&internal_key[internal_key.len() - 8..]);
    value_type_from_tag((tag & 0xff) as u8)
}

pub fn pack_sequence_and_type(sequence: SequenceNumber, value_type: ValueType) -> u64 {
    assert!(sequence <= MAX_SEQUENCE_NUMBER);
    (sequence << 8) | value_type as u64
}

fn value_type_from_tag(tag: u8) -> Option<ValueType> {
    match tag {
        0 => Some(ValueType::Deletion),
        1 => Some(ValueType::Value),
        _ => None,
    }
}

fn required_user_key(internal_key: &[u8]) -> &[u8] {
    extract_user_key(internal_key).expect("internal key requires an 8-byte tag")
}
