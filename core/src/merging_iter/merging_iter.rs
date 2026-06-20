use crate::comparator::Comparator;
use crate::status::Result;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Direction {
    Forward,
    Reverse,
}

/// Merges several sorted child iterators into one. Holds
/// N child iterators (boxed `dyn DbIterator`) and picks the smallest
/// current key on Next, the largest on Prev. After Seek,
/// the children are individually positioned and a linear
/// scan picks the winning child each step.
pub struct MergingIterator<C: Comparator> {
    comparator: C,
    children: Vec<Box<dyn crate::db_iter::DbIterator>>,
    current: Option<usize>,
    direction: Direction,
}

impl<C: Comparator> MergingIterator<C> {
    pub fn new(comparator: C, children: Vec<Box<dyn crate::db_iter::DbIterator>>) -> Self {
        Self {
            comparator,
            children,
            current: None,
            direction: Direction::Forward,
        }
    }

    fn find_smallest(&mut self) {
        let mut smallest: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            let take = match smallest {
                None => true,
                Some(s) => self
                    .comparator
                    .compare(c.key(), self.children[s].key())
                    .is_lt(),
            };
            if take {
                smallest = Some(i);
            }
        }
        self.current = smallest;
    }

    fn find_largest(&mut self) {
        let mut largest: Option<usize> = None;
        for (i, c) in self.children.iter().enumerate() {
            if !c.valid() {
                continue;
            }
            let take = match largest {
                None => true,
                Some(l) => self
                    .comparator
                    .compare(c.key(), self.children[l].key())
                    .is_gt(),
            };
            if take {
                largest = Some(i);
            }
        }
        self.current = largest;
    }
}

impl<C: Comparator> crate::db_iter::DbIterator for MergingIterator<C> {
    fn valid(&self) -> bool {
        self.current.is_some_and(|i| self.children[i].valid())
    }

    fn seek_to_first(&mut self) {
        for c in &mut self.children {
            c.seek_to_first();
        }
        self.find_smallest();
        self.direction = Direction::Forward;
    }

    fn seek_to_last(&mut self) {
        for c in &mut self.children {
            c.seek_to_last();
        }
        self.find_largest();
        self.direction = Direction::Reverse;
    }

    fn seek(&mut self, target: &[u8]) {
        for c in &mut self.children {
            c.seek(target);
        }
        self.find_smallest();
        self.direction = Direction::Forward;
    }

    fn next(&mut self) {
        assert!(self.valid());
        // For backward -> forward transitions, every child
        // not at `current` must seek past `current.key()`.
        if self.direction != Direction::Forward {
            let cur = self.current.expect("valid");
            let key_owned = self.children[cur].key().to_vec();
            for (i, c) in self.children.iter_mut().enumerate() {
                if i == cur {
                    continue;
                }
                c.seek(&key_owned);
                if c.valid() && self.comparator.compare(&key_owned, c.key()).is_eq() {
                    c.next();
                }
            }
            self.direction = Direction::Forward;
        }
        let cur = self.current.expect("valid");
        self.children[cur].next();
        self.find_smallest();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        if self.direction != Direction::Reverse {
            let cur = self.current.expect("valid");
            let key_owned = self.children[cur].key().to_vec();
            for (i, c) in self.children.iter_mut().enumerate() {
                if i == cur {
                    continue;
                }
                c.seek(&key_owned);
                if c.valid() {
                    c.prev();
                } else {
                    c.seek_to_last();
                }
            }
            self.direction = Direction::Reverse;
        }
        let cur = self.current.expect("valid");
        self.children[cur].prev();
        self.find_largest();
    }

    fn key(&self) -> &[u8] {
        self.children[self.current.expect("valid")].key()
    }

    fn value(&self) -> &[u8] {
        self.children[self.current.expect("valid")].value()
    }

    fn status(&self) -> Result<()> {
        for c in &self.children {
            c.status()?;
        }
        Ok(())
    }
}
