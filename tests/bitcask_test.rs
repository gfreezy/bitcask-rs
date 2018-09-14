extern crate bitcask_rs;
extern crate failure;
extern crate log;
extern crate uuid;

use std::fs;
use std::panic;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

fn run_test<T>(test: T) -> ()
where
    T: FnOnce(&str) -> () + panic::UnwindSafe,
{
    setup();

    let uuid = uuid::Uuid::new_v4();
    let path = format!("target/store/{}", uuid.simple().to_string());
    let result = panic::catch_unwind(|| test(&path));

    teardown(&path);

    assert!(result.is_ok())
}

fn setup() {
    bitcask_rs::setup();
}

fn teardown(path: &str) {
    let _ = fs::remove_dir_all(path);
}

#[test]
fn it_can_escape() {
    run_test(|_| {
        assert_eq!(
            bitcask_rs::escape_tombstone("<<>>".as_bytes()),
            "<<>><<>>".as_bytes().to_vec()
        );
        assert_eq!(
            bitcask_rs::escape_tombstone("aa<<>>hel<<>>sdf".as_bytes()),
            "aa<<>><<>>hel<<>><<>>sdf".as_bytes().to_vec()
        );
        assert_eq!(
            bitcask_rs::escape_tombstone("<<>><<>>".as_bytes()),
            "<<>><<>><<>><<>>".as_bytes().to_vec()
        );
    });
}

#[test]
fn it_can_unescape() {
    run_test(|_| {
        assert_eq!(
            bitcask_rs::unescape_tombstone("<<>><<>>".as_bytes()),
            "<<>>".as_bytes().to_vec()
        );
        assert_eq!(
            bitcask_rs::unescape_tombstone("aa<<>><<>>hel<<>><<>>sdf".as_bytes()),
            "aa<<>>hel<<>>sdf".as_bytes().to_vec()
        );
        assert_eq!(
            bitcask_rs::unescape_tombstone("<<>><<>><<>><<>>".as_bytes()),
            "<<>><<>>".as_bytes().to_vec()
        );
        assert_eq!(
            bitcask_rs::unescape_tombstone("<<>>".as_bytes()),
            "<<>>".as_bytes().to_vec()
        );
    })
}

#[test]
fn it_can_parse_config() {
    run_test(|_| {
        let config = bitcask_rs::Config::new("tests/correct_config.yml");
        assert_eq!(config.max_file_id, 1000000000);
        assert_eq!(config.path, PathBuf::from("bitcask/test/store"));
    })
}

#[test]
fn it_cannot_parse_config() {
    run_test(|_| {
        let ret = std::panic::catch_unwind(|| bitcask_rs::Config::new("tests/wrong_config.yml"));
        assert!(ret.is_err());
    })
}

#[test]
fn it_set_a_and_get_a() {
    run_test(|path| {
        let config = bitcask_rs::ConfigBuilder::default()
            .path(PathBuf::from(path))
            .build()
            .unwrap();
        let mut bitcask = bitcask_rs::Bitcask::new(config);
        let key = "1111";
        let set_ret = bitcask.set(key.to_string(), vec![1, 2, 3]);
        assert!(set_ret.is_ok());

        let ret = bitcask.get(&key.to_string());
        assert_eq!(ret.unwrap(), Some(vec![1, 2, 3]));

        let _ = bitcask.delete(key.to_string());
        let ret = bitcask.get(&key.to_string());
        assert_eq!(ret.unwrap(), None);

        let no_exist = bitcask.get(&"hello".to_string());
        assert_eq!(no_exist.unwrap(), None);

        bitcask.set("hello".to_string(), "<<>>".as_bytes().to_vec()).unwrap();
        assert_eq!(
            bitcask.get(&"hello".to_string()).unwrap(),
            Some("<<>>".as_bytes().to_vec())
        );

        bitcask.set("hello".to_string(), "hello<<>><<>>haha".as_bytes().to_vec()).unwrap();
        assert_eq!(
            bitcask.get(&"hello".to_string()).unwrap(),
            Some("hello<<>><<>>haha".as_bytes().to_vec())
        );
    })
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
    run_test(|path| {
        let config = bitcask_rs::ConfigBuilder::default()
            .path(PathBuf::from(path))
            .build()
            .unwrap();
        let mut bitcask = bitcask_rs::Bitcask::new(config);
        populate_store(100, &mut bitcask);
        populate_store(50, &mut bitcask);

        let ret = bitcask.get(&"1".to_string());
        bitcask.merge(None).expect("compact");
        let ret2 = bitcask.get(&"1".to_string());
        assert_eq!(ret.unwrap(), ret2.unwrap());
    })
}

#[test]
fn it_should_build_from_segment_file() {
    run_test(|path| {
        let config = bitcask_rs::ConfigBuilder::default()
            .path(PathBuf::from(path))
            .build()
            .unwrap();
        {
            let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
            populate_store(100, &mut bitcask);
            populate_store(50, &mut bitcask);
        }

        let bitcask = bitcask_rs::Bitcask::open(config);
        let ret = bitcask.get(&"1".to_string());
        assert_eq!(ret.expect("u1").expect("u2"), vec![1, 2, 3, 4, 5]);
    })
}

#[test]
fn it_should_access_from_multiple_thread() {
    run_test(|path| {
        let config = bitcask_rs::ConfigBuilder::default()
            .path(PathBuf::from(path))
            .build()
            .unwrap();
        let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
        populate_store(100, &mut bitcask);
        populate_store(50, &mut bitcask);
        bitcask.set("1".to_string(), vec![1, 3, 4]).unwrap();

        let bitcask_n = bitcask.clone();
        let handler = thread::spawn(move || {
            thread::sleep(Duration::from_secs(2));
            bitcask_n.get(&"1".to_string())
        });

        let mut bitcask_n = bitcask.clone();
        let handler2 = thread::spawn(move || {
            thread::sleep(Duration::from_secs(1));
            bitcask_n.set("1".to_string(), vec![1, 3, 4])
        });

        let ret = bitcask.get(&"1".to_string());

        let ret2 = handler.join().unwrap();
        let _ = handler2.join();
        assert_eq!(ret.expect("u1"), ret2.unwrap());
    })
}

#[test]
fn it_should_compact_while_reading_from_other_thread() {
    run_test(|path| {
        let config = bitcask_rs::ConfigBuilder::default()
            .path(PathBuf::from(path))
            .build()
            .unwrap();
        let mut bitcask = bitcask_rs::Bitcask::new(config.clone());
        populate_store(100, &mut bitcask);
        populate_store(50, &mut bitcask);

        bitcask.set("1".to_string(), vec![1, 3, 4]).unwrap();

        let bitcask_n = bitcask.clone();
        let handler = thread::spawn(move || {
            let mut i = 1000;
            while i > 0 {
                thread::sleep(Duration::from_millis(1));
                assert_eq!(bitcask_n.get(&"1".to_string()).unwrap(), Some(vec![1, 3, 4]));
                i -= 1;
            }
        });

        bitcask.merge(Some(20)).expect("compact");
        assert_eq!(bitcask.get(&"1".to_string()).unwrap(), Some(vec![1, 3, 4]));
        handler.join().unwrap();
    })
}
