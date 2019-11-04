use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[derive(Clone)]

pub struct Database {
    db: Arc<Mutex<BTreeMap<String, String>>>,//使用BTreeMap存储数据库
}

impl Database {
    pub fn new() -> Database {
        Database {
            db: Arc::new(Mutex::new(BTreeMap::new()))
        }
    }
    pub fn get(&self, key: &String) -> Option<String> {
        let db = self.db.lock().unwrap();
        let ret = db.get(key);
        match ret {
            Some(value) => Some(value.to_owned()),
            None => None
        }
    }
    pub fn set(&mut self, key: &String, value: &String) -> Option<String> {
        let mut db = self.db.lock().unwrap();
        db.insert(key.clone(), value.clone())
    }
    pub fn delete(&mut self, key: &String) -> Option<String> {
        let mut db = self.db.lock().unwrap();
        db.remove(key)
    }
    pub fn scan(&self) -> Option<HashMap<String, String>> {
        let mut hmap = HashMap::new();
        let db = self.db.lock().unwrap();
        for (k, v) in db.iter() {
            println!("key: {}, value: {}", k, v);
            hmap.insert(k.clone(), v.clone());
        }
        if !hmap.is_empty() {
            Some(hmap)
        }
        else {
            None
        }
    }
}