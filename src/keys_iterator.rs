use std::sync::RwLockReadGuard;
use store::ActiveData;

pub struct StoreKeys<'a> {
    pub guard: RwLockReadGuard<'a, ActiveData>
}


impl<'a> IntoIterator for &'a StoreKeys<'a> {
    type Item = &'a String;
    type IntoIter = StoreKeysIter<'a>;

    fn into_iter(self) -> <Self as IntoIterator>::IntoIter {
        StoreKeysIter(self.guard.keys())
    }
}


pub struct StoreKeysIter<'a>(Box<Iterator<Item=&'a String> + 'a>);

impl<'a> Iterator for StoreKeysIter<'a> {
    type Item = &'a String;

    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        self.0.next()
    }
}
