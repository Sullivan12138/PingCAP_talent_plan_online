use super::log::Log;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
// when the file size is bigger than this constant, then we should compact it
const COMPACT_SIZE: u16 = 1024;
pub struct Database {
    db: Arc<Mutex<BTreeMap<String, String>>>, // use BTreeMap to save database
}

impl Clone for Database {
    fn clone(&self) -> Database {
        Database {
            db: self.db.clone(),
        }
    }
}

impl Database {
    // create a new database
    pub fn new() -> Database {
        let mut database = Database {
            db: Arc::new(Mutex::new(BTreeMap::new())),
        };
        // then restore the data till closing the server according to the log file
        let ret = database.update();
        match ret {
            Ok(_t) => println!("updating database successfully"),
            Err(_e) => println!("error  updating database"),
        };
        database
    }
    /// the get operation of database
    pub fn get(&self, key: &String) -> Result<Option<String>, ()> {
        let db = self.db.lock().unwrap();
        let ret = db.get(key);
        match ret {
            Some(value) => Ok(Some(value.to_owned())),
            None => Ok(None),
        }
    }
    /// the set operation of database
    pub fn set(&mut self, key: &String, value: &String) -> Result<Option<String>, ()> {
        let mut db = self.db.lock().unwrap();
        // write to the log first, then write the data
        let log = Log::new(0, key.clone(), value.clone());
        let ret = log.write_log();
        match ret {
            Ok(_) => {}
            Err(e) => {
                println!("write file error {:?}", e);
                return Err(());
            }
        }
        let ret = db.insert(key.clone(), value.clone());
        match ret {
            Some(s) => Ok(Some(s)),
            None => Ok(None),
        }
    }
    /// the delete operation of database
    pub fn delete(&mut self, key: &String) -> Result<Option<String>, ()> {
        let mut db = self.db.lock().unwrap();
        let value: String = "".to_owned();
        let log = Log::new(1, key.clone(), value.clone());
        let ret = log.write_log();
        match ret {
            Ok(_) => {}
            Err(e) => {
                println!("write file error {:?}", e);
                return Err(());
            }
        }
        let ret = db.remove(key);
        match ret {
            Some(s) => Ok(Some(s)),
            None => Ok(None),
        }
    }
    /// the scan operation of database
    pub fn scan(
        &self,
        key_start: &String,
        key_end: &String,
    ) -> Result<Option<HashMap<String, String>>, ()> {
        let mut hmap = HashMap::new();
        for (k, v) in self
            .db
            .lock()
            .unwrap()
            .range(key_start.clone()..key_end.clone())
        {
            //println!("scan[{}:{}]", k, v);
            hmap.insert(k.clone(), v.clone());
        }
        if hmap.len() != 0 {
            Ok(Some(hmap))
        } else {
            Ok(None)
        }
    }
    /// update the database using database file and log file
    /// this method works every time when server starts
    /// it is like udating the database using REDO list from the last check point
    pub fn update(&mut self) -> io::Result<()> {
        println!("updating the database, please wait...");
        let path = Path::new("log.txt");
        let file: File;
        let ret = File::open(path);
        match ret {
            Ok(o) => {
                file = o;
            }
            Err(_) => return Ok(()),
        }
        let logs = Log::read_log(file);
        let mut db = self.db.lock().unwrap();
        // REDO list
        let mut count: u64 = 0;
        for log in logs.iter() {
            count = count + 1;
            if log.get_change_type() == 0 {
                db.insert(log.get_key(), log.get_value());
            //                println!("insert into the database, key:{}, value:{}", log.get_key(), log.get_value())
            } else {
                db.remove(&log.get_key());
                //                println!("delete from the database, key:{}", log.get_key());
            }
        }
        if count > COMPACT_SIZE as u64 {
            fs::remove_file("log.txt");
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open("log.txt".to_owned())
                .unwrap();
            for (k, v) in db.iter() {
                Log::write_log(&Log::new(0, k.clone(), v.clone()));
            }
        }
        Ok(())
    }
}
