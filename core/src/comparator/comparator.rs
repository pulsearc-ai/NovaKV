use std::cmp::Ordering;

pub trait Comparator {
    fn name(&self) -> &'static str;
    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering;
    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]);
    fn find_short_successor(&self, key: &mut Vec<u8>);
}

#[derive(Debug, Copy, Clone, Default)]
pub struct BytewiseComparator;

impl Comparator for BytewiseComparator {
    fn name(&self) -> &'static str {
        "novakv.BytewiseComparator"
    }

    fn compare(&self, a: &[u8], b: &[u8]) -> Ordering {
        a.cmp(b)
    }

    fn find_shortest_separator(&self, start: &mut Vec<u8>, limit: &[u8]) {
        let min_len = start.len().min(limit.len());
        let mut diff_index = 0;
        while diff_index < min_len && start[diff_index] == limit[diff_index] {
            diff_index += 1;
        }

        if diff_index < min_len {
            let diff_byte = start[diff_index];
            if diff_byte < 0xff && diff_byte + 1 < limit[diff_index] {
                start[diff_index] += 1;
                start.truncate(diff_index + 1);
                debug_assert!(self.compare(start, limit).is_lt());
            }
        }
    }

    fn find_short_successor(&self, key: &mut Vec<u8>) {
        for index in 0..key.len() {
            if key[index] != 0xff {
                key[index] += 1;
                key.truncate(index + 1);
                return;
            }
        }
    }
}
