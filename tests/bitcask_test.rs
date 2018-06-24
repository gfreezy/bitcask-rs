extern crate bitcask_rs;
extern crate failure;
#[macro_use]
extern crate log;

use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use std::fs;


fn setup_test() -> Result<(), failure::Error> {
    bitcask_rs::setup();
    for entry in fs::read_dir("target")? {
        let entry = entry?;

        if entry.path().to_string_lossy().starts_with("target/store") {
            fs::remove_dir_all(entry.path())?;
            debug!("rmdir {:?}", entry.path());
        }
    }
    Ok(())
}


#[test]
fn it_set_a_and_get_a() {
    let _ = setup_test();
    let config = bitcask_rs::ConfigBuilder::default().path(PathBuf::from("target/store")).build().unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config);
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
    let _ = setup_test();
    let config = bitcask_rs::ConfigBuilder::default().path(PathBuf::from("target/store2")).build().unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config);
    populate_store(100, &mut bitcask);
    populate_store(50, &mut bitcask);

    let ret = bitcask.get("1".to_string());
    bitcask.merge(None).expect("compact");
    let ret2 = bitcask.get("1".to_string());
    assert_eq!(ret.unwrap(), ret2.unwrap());
}

#[test]
fn it_should_build_from_segment_file() {
    let _ = setup_test();
    let config = bitcask_rs::ConfigBuilder::default().path(PathBuf::from("target/store3")).build().unwrap();
    {
        let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
        populate_store(100, &mut bitcask);
        populate_store(50, &mut bitcask);
    }

    let bitcask = bitcask_rs::Bitcask::open(config);
    let ret = bitcask.get("1".to_string());
    assert_eq!(ret.expect("u1").expect("u2"), vec![1, 2, 3, 4, 5]);
}

#[test]
fn it_should_access_from_multiple_thread() {
    let _ = setup_test();
    let config = bitcask_rs::ConfigBuilder::default().path(PathBuf::from("target/store4")).build().unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
    populate_store(100, &mut bitcask);
    populate_store(50, &mut bitcask);

    let bitcask_n = bitcask.clone();
    let handler = thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        bitcask_n.get("1".to_string())
    });

    let mut bitcask_n = bitcask.clone();
    let handler2 = thread::spawn(move || {
        thread::sleep(Duration::from_secs(1));
        bitcask_n.set("1".to_string(), vec![1, 3, 4])
    });

    let ret = bitcask.get("1".to_string());

    let ret2 = handler.join().unwrap();
    let _ = handler2.join();
    assert_eq!(ret.expect("u1"), ret2.unwrap());
}


#[test]
fn it_should_compact_while_reading_from_other_thread() {
    let _ = setup_test();
    let config = bitcask_rs::ConfigBuilder::default().path(PathBuf::from("target/store5")).build().unwrap();
    let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
    populate_store(100, &mut bitcask);
    populate_store(50, &mut bitcask);

    bitcask.set("1".to_string(), vec![1, 3, 4]).unwrap();

    let bitcask_n = bitcask.clone();
    let handler = thread::spawn(move || {
        let mut i = 1000;
        while i > 0 {
            thread::sleep(Duration::from_millis(1));
            assert_eq!(bitcask_n.get("1".to_string()).unwrap(), Some(vec![1, 3, 4]));
            i -= 1;
        }
    });

    bitcask.merge(Some(20)).expect("compact");
    assert_eq!(bitcask.get("1".to_string()).unwrap(), Some(vec![1, 3, 4]));
    handler.join().unwrap();
}
