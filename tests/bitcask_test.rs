extern crate bitcask_rs;
extern crate failure;
extern crate log;
extern crate simple_logger;

use std::path::PathBuf;

#[test]
fn it_set_a_and_get_a() {
    simple_logger::init().unwrap();

    let mut bitcask = bitcask_rs::Bitcask::new(PathBuf::from("store"));
    let key = "1111";
    let set_ret = bitcask.set(key.to_string(), vec![1, 2, 3]);
    assert!(set_ret.is_ok());

    let ret = bitcask.get(key.to_string());
    assert_eq!(ret.unwrap(), Some(vec![1, 2, 3]));

    let _ = bitcask.delete(key.to_string());
    let ret = bitcask.get(key.to_string());
    assert_eq!(ret.unwrap(), None);

    let no_exist = bitcask.get("hello".to_string());
    assert_eq!(no_exist.unwrap(), None);
}


fn populate_store(end: u8, bitcask: &mut bitcask_rs::Bitcask) {
    for i in 1..end {
        let key = format!("{}", i);
        let value = (i..(i + 5)).collect();
        bitcask.set(key.to_string(), value).unwrap();
    }
}


#[test]
fn it_should_compact() {
    let mut bitcask = bitcask_rs::Bitcask::new(PathBuf::from("store3"));
    populate_store(100, &mut bitcask);
    populate_store(50, &mut bitcask);

    let ret = bitcask.get("1".to_string());
    bitcask.compact().expect("compact");
    let ret2 = bitcask.get("1".to_string());
    assert_eq!(ret.unwrap(), ret2.unwrap());
}
