use std::collections::HashSet;
use std::sync::RwLockReadGuard;
use store::{ActiveData, OlderData};

pub struct StoreKeys<'a> {
    pub active_data_guard: RwLockReadGuard<'a, ActiveData>,
    pub older_data_guard: RwLockReadGuard<'a, OlderData>,
}

impl<'a> IntoIterator for &'a StoreKeys<'a> {
    type Item = &'a String;
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
    iter: Box<Iterator<Item = &'a String> + 'a>,
    seen: HashSet<String>,
}

impl<'a> StoreKeysIter<'a> {
    fn new(iter: Box<Iterator<Item = &'a String> + 'a>) -> StoreKeysIter<'a> {
        StoreKeysIter {
            iter,
            seen: HashSet::new(),
        }
    }
}

impl<'a> Iterator for StoreKeysIter<'a> {
    type Item = &'a String;

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
