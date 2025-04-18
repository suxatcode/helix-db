use std::sync::Arc;

use heed3::{RoTxn, RwTxn};

use crate::{
    helix_engine::storage_core::{
        storage_core::HelixGraphStorage, storage_methods::StorageMethods,
    },
    protocol::{
        filterable::{Filterable, FilterableType},
        items::{Edge, Node},
    },
};

pub struct Range<I> {
    iter: I,
    curr_idx: usize,
    start: usize,
    end: usize,
}

// implementing iterator for Range
impl<I> Iterator for Range<I>
where
    I: Iterator,
{
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        // skips to start
        while self.curr_idx < self.start {
            match self.iter.next() {
                Some(_) => self.curr_idx += 1,
                None => return None, // out of items
            }
        }
        
        // return between start and end
        if self.curr_idx < self.end {
            match self.iter.next() {
                Some(item) => {
                    self.curr_idx += 1;
                    Some(item)
                }
                None => None,
            }
        } else {
            // all consumed
            None
        }
    }
}

pub trait RangeAdapter: Iterator {
    /// Range returns a slice of the current step between two points
    fn range(self, start: usize, end: usize) -> Range<Self>
    where
        Self: Sized + Iterator,
        Self::Item: Send,
    {
        Range {
            iter: self,
            curr_idx: 0,
            start,
            end,
            }
        }
    }

impl<T: ?Sized> RangeAdapter for T where T: Iterator {}
