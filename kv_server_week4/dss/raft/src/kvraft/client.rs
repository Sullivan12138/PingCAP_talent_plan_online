use futures::Future;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::service;
#[macro_export]
macro_rules! my_debug2 {
    ($($arg: tt)*) => {
        // println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    };
}
const NEXT_RAFT_INTERVAL: u64 = 110;
const NEXT_LEADER_INTERVAL: u64 = 50;

enum Op {
    Put(String, String),
    Append(String, String),
}

pub struct Clerk {
    pub name: String,
    servers: Vec<service::KvClient>,
    // You will have to modify this struct.
    seq: Arc<Mutex<u64>>, // the sequence number of request
    leader_id: Arc<Mutex<u64>>,
}

impl fmt::Debug for Clerk {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Clerk").field("name", &self.name).finish()
    }
}

impl Clerk {
    pub fn new(name: String, servers: Vec<service::KvClient>) -> Clerk {
        // You'll have to add code here.
        let clerk = Clerk {
            name: name.clone(),
            servers,
            seq: Arc::new(Mutex::new(0)),
            leader_id: Arc::new(Mutex::new(0)),
        };
        my_debug2!("new client: {}", name);
        clerk
    }

    pub fn get_seq(&self) -> u64 {
        let mut seq = self.seq.lock().unwrap();
        *seq += 1;
        *seq
    }
    /// fetch the current value for a key.
    /// returns "" if the key does not exist.
    /// keeps trying forever in the face of all other errors.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].get(args).unwrap();
    pub fn get(&self, key: String) -> String {
        // You will have to modify this function.
        let args = service::GetRequest {
            key: key.clone(),
            seq: self.get_seq(),
            name: self.name.clone(),
        };
        let mut i = *self.leader_id.lock().unwrap() as usize;
        my_debug2!(
            "client {} get request, key: {}, seq: {}",
            self.name,
            key.clone(),
            self.seq.lock().unwrap()
        );
        loop {
            let reply = self.servers[i].get(&args).wait();
            match reply {
                Ok(t) => {
                    if t.err == "OK" {
                        if !t.wrong_leader {
                            *self.leader_id.lock().unwrap() = i as u64;
                            my_debug2!("success reply! leader_id: {}, value: {}", i, t.value);
                        }

                        return t.value;
                    } else {
                        if !t.wrong_leader {
                            *self.leader_id.lock().unwrap() = i as u64;
                            thread::sleep(Duration::from_millis(NEXT_RAFT_INTERVAL));
                        } else {
                            i = (i + 1) % self.servers.len();
                            thread::sleep(Duration::from_millis(NEXT_LEADER_INTERVAL));
                        }
                    }
                }
                Err(_e) => {
                    i = (i + 1) % self.servers.len();
                    thread::sleep(Duration::from_millis(NEXT_LEADER_INTERVAL));
                }
            }
        }
    }

    /// shared by Put and Append.
    //
    // you can send an RPC with code like this:
    // let reply = self.servers[i].put_append(args).unwrap();
    fn put_append(&self, op: Op) {
        // You will have to modify this function.
        let key;
        let value;
        let op_type;
        match op {
            Op::Put(k, v) => {
                key = k;
                value = v;
                op_type = service::Op::Put;
            }
            Op::Append(k, v) => {
                key = k;
                value = v;
                op_type = service::Op::Append;
            }
        }
        let args = service::PutAppendRequest {
            key,
            value,
            op: op_type as i32,
            seq: self.get_seq(),
            name: self.name.clone(),
        };
        if args.op == 1 {
            my_debug2!(
                "client {} put, key: {}, value: {}, seq: {}",
                args.name,
                args.key,
                args.value,
                args.seq
            );
        }
        let mut i = *self.leader_id.lock().unwrap() as usize;
        loop {
            let reply = self.servers[i].put_append(&args).wait();
            match reply {
                Ok(t) => {
                    if t.err == "OK" {
                        if !t.wrong_leader {
                            *self.leader_id.lock().unwrap() = i as u64;
                        }
                        return;
                    } else {
                        if !t.wrong_leader {
                            *self.leader_id.lock().unwrap() = i as u64;
                            thread::sleep(Duration::from_millis(NEXT_RAFT_INTERVAL));
                        } else {
                            i = (i + 1) % self.servers.len();
                            thread::sleep(Duration::from_millis(NEXT_LEADER_INTERVAL));
                        }
                    }
                }
                Err(_e) => {
                    i = (i + 1) % self.servers.len();
                    thread::sleep(Duration::from_millis(NEXT_LEADER_INTERVAL));
                }
            }
        }
    }

    pub fn put(&self, key: String, value: String) {
        self.put_append(Op::Put(key, value))
    }

    pub fn append(&self, key: String, value: String) {
        self.put_append(Op::Append(key, value))
    }
}
