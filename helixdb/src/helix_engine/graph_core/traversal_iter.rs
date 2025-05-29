use std::sync::Arc;

use heed3::{RoTxn, RwTxn, WithoutTls};

use super::ops::tr_val::TraversalVal;
use crate::helix_engine::{storage_core::storage_core::HelixGraphStorage, types::GraphError};
use itertools::Itertools;

pub struct RoTraversalIterator<'a, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'a RoTxn<'a, WithoutTls>,
}

// implementing iterator for TraversalIterator
impl<'a, I> Iterator for RoTraversalIterator<'a, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}

impl<'a, I: Iterator<Item = Result<TraversalVal, GraphError>>> RoTraversalIterator<'a, I> {
    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_dedup<B: FromIterator<TraversalVal>>(self) -> B {
        self.inner
            .filter_map(|item| item.ok())
            .unique()
            .collect::<B>()
    }

    pub fn collect_parallel(
        self
    ) -> Result<Vec<TraversalVal>, GraphError> {
        let iters = (0..8).map(|_| self.enumerate()).collect::<Vec<_>>();
    
        std::thread::scope(|s| {
            let n = 8; // number of threads
    
            let threads: Vec<_> = iters
                .into_iter()
                .enumerate()
                .map(|(x, iter)| s.spawn(move || iterate(n, x, iter)))
                .collect();
            let results: Result<Vec<_>, GraphError> =
                threads.into_iter().map(|t| t.join().unwrap()).collect();
    
            // Flatten all results
            Ok(results?.into_iter().flatten().collect())
        })
    }

    pub fn collect_to_obj(self) -> Option<TraversalVal> {
        self.inner.filter_map(|item| item.ok()).take(1).next()
    }
}
pub struct RwTraversalIterator<'scope, 'env, I> {
    pub inner: I,
    pub storage: Arc<HelixGraphStorage>,
    pub txn: &'scope mut RwTxn<'env>,
}

// implementing iterator for TraversalIterator
impl<'scope, 'env, I> Iterator for RwTraversalIterator<'scope, 'env, I>
where
    I: Iterator<Item = Result<TraversalVal, GraphError>>,
{
    type Item = Result<TraversalVal, GraphError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }
}
impl<'scope, 'env, I: Iterator> RwTraversalIterator<'scope, 'env, I> {
    pub fn new(storage: Arc<HelixGraphStorage>, txn: &'scope mut RwTxn<'env>, inner: I) -> Self {
        Self {
            inner,
            storage,
            txn,
        }
    }

    

    pub fn collect_to<B: FromIterator<TraversalVal>>(self) -> B
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        self.inner.filter_map(|item| item.ok()).collect::<B>()
    }

    pub fn collect_to_val(self) -> TraversalVal
    where
        I: Iterator<Item = Result<TraversalVal, GraphError>>,
    {
        match self
            .inner
            .filter_map(|item| item.ok())
            .collect::<Vec<_>>()
            .first()
        {
            Some(val) => val.clone(), // TODO: Remove clone
            None => TraversalVal::Empty,
        }
    }
}
// pub trait TraversalIteratorMut<'a> {
//     type Inner: Iterator<Item = Result<TraversalVal, GraphError>>;

//     fn next<'b>(
//         &mut self,
//         storage: Arc<HelixGraphStorage>,
//         txn: &'b mut RwTxn<'a>,
//     ) -> Option<Result<TraversalVal, GraphError>>;

// }
fn iterate<'t>(
    n: usize, // number of threads
    x: usize, // thread ID
    iter: impl Iterator<Item = (usize, Result<TraversalVal, GraphError>)>,
) -> Result<Vec<TraversalVal>, GraphError> {
    // i % n == x where
    //  i is the increment returned by the enumerator
    //  n is the total number or working threads and
    //  x is the thread ID on which we are.
    let mut count = 0;

    let capacity = iter.size_hint().1.unwrap_or(0);

    let mut vals = Vec::with_capacity(capacity);
    for (i, result) in iter {
        if i % n == x {
            let val = result?;
            vals.push(val);
            count += 1;
        }
    }

    eprintln!("thread {x} has seen {count} keys");

    Ok(vals)
}


