use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, RwLock};

#[derive(Clone)]
pub struct DbEngine{
    db: Arc<RwLock<BTreeMap<String,String>>>,
}

impl DbEngine{
    pub fn new() -> Self {
        println!("new db");
        DbEngine {
            db: Arc::new(RwLock::new(BTreeMap::new())),
        }
    }
    pub fn get(&self, key: &String) -> Result<Option<String>,()> {
            let db = self.db.read().unwrap();
            let ret = db.get(key);
            match ret {
                Some(s) => Ok(Some(s.clone())),
                None => Ok(None)
            }
        
    }
    pub fn put(&mut self, key: &String, value: &String) -> Result<i32,()> {
            let ret = self.db.write().unwrap().insert(key.clone(),value.clone());
            match ret {
                Some(_) => Ok(1),  //有覆盖的key
                None => Ok(0)
            }
        
    }
    pub fn delete(&mut self, key: &String) -> Result<Option<String>,()> {
            let ret = self.db.write().unwrap().remove(key);
            match ret {
                Some(s) => Ok(Some(s)),  //删除存在的key-value
                None => Ok(None)         //删除不存在的key-value
            }
        
    }
    pub fn scan(&self, key_start: &String, key_end: &String) -> Result<Option<HashMap<String,String>>,()> {
            let mut hmap = HashMap::new();
            for (k, v) in self.db.read().unwrap().range(key_start.clone()..key_end.clone()){
                println!("scan[{}:{}]", k, v);
                hmap.insert(k.clone(),v.clone());
            }
            if hmap.len() != 0 {
                Ok(Some(hmap))
            }
            else {
                Ok(None)
            }
        
    }
}