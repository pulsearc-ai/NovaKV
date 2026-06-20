use crate::status::Result;

pub trait DbIterator {
    fn valid(&self) -> bool;
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn seek(&mut self, target: &[u8]);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> &[u8];
    fn value(&self) -> &[u8];
    fn status(&self) -> Result<()>;
}
