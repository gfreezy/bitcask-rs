use std::collections::HashSet;
use std::sync::RwLockReadGuard;
use store::{ActiveData, OlderData};
use core::Key;

pub struct StoreKeys<'a> {
    pub active_data_guard: RwLockReadGuard<'a, ActiveData>,
    pub older_data_guard: RwLockReadGuard<'a, OlderData>,
}

impl<'a> IntoIterator for &'a StoreKeys<'a> {
    type Item = &'a Key;
    type IntoIter = StoreKeysIter<'a>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        StoreKeysIter::new(Box::new(
            self.active_data_guard
                .keys()
                .chain(self.older_data_guard.keys()),
        ))
    }
}

pub struct StoreKeysIter<'a> {
    iter: Box<Iterator<Item = &'a Key> + 'a>,
    seen: HashSet<Key>,
}

impl<'a> StoreKeysIter<'a> {
    fn new(iter: Box<Iterator<Item = &'a Key> + 'a>) -> StoreKeysIter<'a> {
        StoreKeysIter {
            iter,
            seen: HashSet::new(),
        }
    }
}

impl<'a> Iterator for StoreKeysIter<'a> {
    type Item = &'a Key;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        loop {
            let v = self.iter.next();
            let i = v?;
            if self.seen.insert(i.clone()) {
                return v;
            } else {
                continue;
            }
        }
    }
}
