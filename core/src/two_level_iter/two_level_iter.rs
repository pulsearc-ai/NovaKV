use crate::status::Result;

/// Two-level iterator over an index and its data blocks.
/// The outer
/// iterator yields `(key, BlockHandle bytes)` pairs; on
/// each transition the `block_function` closure is called
/// to load the inner iterator for that data block.
pub struct TwoLevelIterator<O, I, F>
where
    O: crate::db_iter::DbIterator,
    I: crate::db_iter::DbIterator,
    F: FnMut(&[u8]) -> Result<I>,
{
    index_iter: O,
    data_iter: Option<I>,
    block_function: F,
    /// Cached block-handle bytes that produced the current data_iter.
    data_block_handle: Vec<u8>,
    status: Result<()>,
}

impl<O, I, F> TwoLevelIterator<O, I, F>
where
    O: crate::db_iter::DbIterator,
    I: crate::db_iter::DbIterator,
    F: FnMut(&[u8]) -> Result<I>,
{
    pub fn new(index_iter: O, block_function: F) -> Self {
        Self {
            index_iter,
            data_iter: None,
            block_function,
            data_block_handle: Vec::new(),
            status: Ok(()),
        }
    }

    fn save_status(&mut self) {
        if self.status.is_ok() {
            let st = self.index_iter.status();
            if st.is_err() {
                self.status = st;
                return;
            }
            if let Some(d) = &self.data_iter {
                let st = d.status();
                if st.is_err() {
                    self.status = st;
                }
            }
        }
    }

    fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.data_iter = None;
            return;
        }
        let handle = self.index_iter.value().to_vec();
        if self.data_iter.is_some() && handle == self.data_block_handle {
            // Same block as before; reuse the iterator.
            return;
        }
        match (self.block_function)(&handle) {
            Ok(it) => {
                self.data_iter = Some(it);
                self.data_block_handle = handle;
            }
            Err(e) => {
                self.data_iter = None;
                self.status = Err(e);
            }
        }
    }

    fn skip_empty_data_blocks_forward(&mut self) {
        while self.data_iter.as_ref().is_none_or(|d| !d.valid()) {
            self.save_status();
            if self.status.is_err() {
                return;
            }
            if !self.index_iter.valid() {
                self.data_iter = None;
                return;
            }
            self.index_iter.next();
            self.init_data_block();
            if let Some(d) = &mut self.data_iter {
                d.seek_to_first();
            }
        }
    }

    fn skip_empty_data_blocks_backward(&mut self) {
        while self.data_iter.as_ref().is_none_or(|d| !d.valid()) {
            self.save_status();
            if self.status.is_err() {
                return;
            }
            if !self.index_iter.valid() {
                self.data_iter = None;
                return;
            }
            self.index_iter.prev();
            self.init_data_block();
            if let Some(d) = &mut self.data_iter {
                d.seek_to_last();
            }
        }
    }
}

impl<O, I, F> crate::db_iter::DbIterator for TwoLevelIterator<O, I, F>
where
    O: crate::db_iter::DbIterator,
    I: crate::db_iter::DbIterator,
    F: FnMut(&[u8]) -> Result<I>,
{
    fn valid(&self) -> bool {
        self.data_iter.as_ref().is_some_and(|d| d.valid())
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        self.init_data_block();
        if let Some(d) = &mut self.data_iter {
            d.seek_to_first();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn seek_to_last(&mut self) {
        self.index_iter.seek_to_last();
        self.init_data_block();
        if let Some(d) = &mut self.data_iter {
            d.seek_to_last();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn seek(&mut self, target: &[u8]) {
        self.index_iter.seek(target);
        self.init_data_block();
        if let Some(d) = &mut self.data_iter {
            d.seek(target);
        }
        self.skip_empty_data_blocks_forward();
    }

    fn next(&mut self) {
        assert!(self.valid());
        if let Some(d) = &mut self.data_iter {
            d.next();
        }
        self.skip_empty_data_blocks_forward();
    }

    fn prev(&mut self) {
        assert!(self.valid());
        if let Some(d) = &mut self.data_iter {
            d.prev();
        }
        self.skip_empty_data_blocks_backward();
    }

    fn key(&self) -> &[u8] {
        self.data_iter.as_ref().expect("valid()").key()
    }

    fn value(&self) -> &[u8] {
        self.data_iter.as_ref().expect("valid()").value()
    }

    fn status(&self) -> Result<()> {
        self.status.clone()?;
        self.index_iter.status()?;
        if let Some(d) = &self.data_iter {
            d.status()?;
        }
        Ok(())
    }
}
