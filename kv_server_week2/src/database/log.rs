use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::string::String;

#[derive(Serialize, Deserialize, Clone)]
pub struct Log {
    change_type: u8,
    key: String,
    value: String,
}

impl Log {
    /// create a new log
    pub fn new(change_type: u8, key: String, value: String) -> Self {
        Log {
            change_type,
            key,
            value,
        }
    }
    /// get the operation type, 0 for set, 1 for delete
    pub fn get_change_type(&self) -> u8 {
        self.change_type
    }
    /// get the key of a log
    pub fn get_key(&self) -> String {
        self.key.clone()
    }
    /// get the value of a log
    pub fn get_value(&self) -> String {
        self.value.clone()
    }
    /// write the log into file
    pub fn write_log(&self) -> io::Result<()> {
        // tests if writing log will block main net's IO
        // thread::sleep(Duration::from_secs(5));
        let dt = Local::now();
        let time_stamp = dt.timestamp();
        let nano_sec = dt.timestamp_subsec_nanos();
        let f = OpenOptions::new()
            .create(true)
            .append(true)
            .open("log.txt".to_owned())
            .unwrap();
        let mut writer = BufWriter::new(f);
        serde_json::to_writer(writer, &self);
        Ok(())
    }
    /// read logs from file
    pub fn read_log(file: File) -> Vec<Log> {
        // tests if reading log will block main net's IO
        // thread::sleep(Duration::from_secs(5));
        let mut reader = BufReader::new(file);
        let mut stream = Deserializer::from_reader(reader).into_iter::<Log>();

        let mut logs: Vec<Log> = vec![];

        while let Some(log) = stream.next() {
            match log {
                Ok(o) => {
                    logs.push(o);
                }
                Err(e) => {
                    println!("error load log:{:?}", e);
                }
            }
        }
        return logs;
    }
}
