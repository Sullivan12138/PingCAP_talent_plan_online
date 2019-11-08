use super::service::*;
use crate::raft;

use futures::stream::Stream;
use futures::sync::mpsc::{unbounded, UnboundedReceiver};
use futures::Async;
use labrpc::RpcFuture;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::thread;
#[macro_export]
macro_rules! my_debug3 {
    ($($arg: tt)*) => {
        // println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    };
}
#[derive(Clone, PartialEq, Message)]
pub struct Reply {
    #[prost(uint64, tag = "1")]
    pub seq: u64, // the sequence number of request

    #[prost(string, tag = "2")]
    pub value: String, // the result of get
}

impl Reply {
    pub fn new() -> Reply {
        Reply {
            seq: 0,
            value: String::new(),
        }
    }
}
#[derive(Clone, PartialEq, Message)]
pub struct OpEntry {
    // log structure
    #[prost(uint64, tag = "1")]
    pub seq: u64, // sequence number
    #[prost(string, tag = "2")]
    pub name: String, // name of client
    #[prost(uint64, tag = "3")]
    pub op: u64, // operation type, 0 for get, 1 for put, 2 for append
    #[prost(string, tag = "4")]
    pub key: String,
    #[prost(string, tag = "5")]
    pub value: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Snapshot {
    // save snapshot structure
    #[prost(uint64, tag = "1")]
    pub snapshot_index: u64,
    // save hashmap's key and value respectively
    #[prost(bytes, repeated, tag = "2")]
    pub key: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "3")]
    pub value: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "4")]
    pub requests_key: Vec<Vec<u8>>,

    #[prost(bytes, repeated, tag = "5")]
    pub requests_value: Vec<Vec<u8>>,
}

pub struct KvServer {
    pub rf: raft::Node,
    me: usize,
    // snapshot if log grows this big
    maxraftstate: Option<usize>,
    // Your definitions here.
    pub apply_ch: UnboundedReceiver<raft::ApplyMsg>,
    pub database: HashMap<String, String>, // database, stored in hashmap
    pub requests: HashMap<String, Reply>,  // each request of client
    pub snapshot_index: u64,
}

impl KvServer {
    pub fn new(
        servers: Vec<raft::service::RaftClient>,
        me: usize,
        persister: Box<dyn raft::persister::Persister>,
        maxraftstate: Option<usize>,
    ) -> KvServer {
        // You may need initialization code here.
        let snapshot = persister.snapshot();
        let (tx, apply_ch) = unbounded();
        let rf = raft::Raft::new(servers, me, persister, tx);

        let mut kv_server = KvServer {
            rf: raft::Node::new(rf),
            me,
            maxraftstate,
            apply_ch,
            database: HashMap::new(),
            requests: HashMap::new(),
            snapshot_index: 0,
        };
        kv_server.load_snapshot(snapshot);
        kv_server
    }
    pub fn get_state(&self) -> raft::State {
        self.rf.get_state()
    }
    pub fn create_snapshot(&self) -> Vec<u8> {
        let mut data = vec![];
        let mut snapshot = Snapshot {
            snapshot_index: self.snapshot_index,
            key: vec![],
            value: vec![],
            requests_key: vec![],
            requests_value: vec![],
        };
        for (key, value) in &self.database {
            let mut k = vec![];
            let mut v = vec![];
            let _ret = labcodec::encode(&key.clone(), &mut k);
            let _ret2 = labcodec::encode(&value.clone(), &mut v);
            snapshot.key.push(k);
            snapshot.value.push(v);
        }
        for (key, value) in &self.requests {
            let mut k = vec![];
            let mut v = vec![];
            let _ret = labcodec::encode(&key.clone(), &mut k);
            let _ret2 = labcodec::encode(&value.clone(), &mut v);
            snapshot.requests_key.push(k);
            snapshot.requests_value.push(v);
        }
        let _ret = labcodec::encode(&snapshot, &mut data);
        data
    }
    pub fn load_snapshot(&mut self, data: Vec<u8>) {
        if data.is_empty() {
            return;
        } else {
            match labcodec::decode(&data) {
                Ok(o) => {
                    let snapshot: Snapshot = o;
                    self.snapshot_index = snapshot.snapshot_index;
                    self.database.clear();
                    let mut key: String = String::new();
                    let mut value: String = String::new();
                    let mut value2: Reply = Reply::new();
                    for i in 0..snapshot.key.len() {
                        match labcodec::decode(&snapshot.key[i]) {
                            Ok(o) => {
                                let k: String = o;
                                key = k;
                            }
                            Err(_e) => {}
                        }
                        match labcodec::decode(&snapshot.value[i]) {
                            Ok(o) => {
                                let v: String = o;
                                value = v;
                            }
                            Err(_e) => {}
                        }
                        self.database.insert(key.clone(), value.clone());
                    }
                    for i in 0..snapshot.requests_key.len() {
                        match labcodec::decode(&snapshot.requests_key[i]) {
                            Ok(o) => {
                                let k: String = o;
                                key = k;
                            }
                            Err(_e) => {}
                        }
                        match labcodec::decode(&snapshot.requests_value[i]) {
                            Ok(o) => {
                                let v: Reply = o;
                                value2 = v;
                            }
                            Err(_e) => {}
                        }
                        self.requests.insert(key.clone(), value2.clone());
                    }
                }
                Err(_e) => {}
            }
        }
    }
    pub fn save_snapshot(&self) {
        let data = self.create_snapshot();
        self.rf.save_snapshot(data);
    }
    pub fn is_need_compress(&self) -> bool {
        match self.maxraftstate {
            Some(maxraftstate) => {
                if maxraftstate > 0 {
                    return true;
                }
                return false;
            }
            None => {
                return false;
            }
        }
    }
}

// Choose concurrency paradigm.
//
// You can either drive the kv server by the rpc framework,
//
// ```rust
// struct Node { server: Arc<Mutex<KvServer>> }
// ```
//
// or spawn a new thread runs the kv server and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your definitions here.
    server: Arc<Mutex<KvServer>>,
    apply_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    shutdown: Arc<Mutex<bool>>,
}

impl Node {
    pub fn new(kv: KvServer) -> Node {
        // Your code here.
        let node = Node {
            server: Arc::new(Mutex::new(kv)),
            apply_thread: Arc::new(Mutex::new(None)),
            shutdown: Arc::new(Mutex::new(false)),
        };
        Node::create_apply_thread(node.clone());
        node
    }
    pub fn create_apply_thread(node: Node) {
        let node2 = node.clone();
        let thread1 = thread::spawn(move || {
            loop {
                if *node2.shutdown.lock().unwrap() == true {
                    break;
                }
                // deal with the request asynchronously
                if let Ok(Async::Ready(Some(apply_msg))) =
                futures::executor::spawn(futures::lazy(|| {
                    node2.server.lock().unwrap().apply_ch.poll()
                }))
                    .wait_future()
                {
                    if !apply_msg.command_valid {
                        continue;
                    }
                    let mut server = node2.server.lock().unwrap();
                    if apply_msg.is_snapshot {
                        if server.snapshot_index < apply_msg.command_index {
                            server.load_snapshot(apply_msg.data.clone());
                        }
                        continue;
                    }
                    if apply_msg.command.is_empty()
                        || apply_msg.command_index <= server.snapshot_index
                    {
                        continue;
                    }
                    let entry: OpEntry;
                    match labcodec::decode(&apply_msg.command) {
                        Ok(o) => {
                            entry = o;
                        }
                        Err(_e) => {
                            continue;
                        }
                    }
                    // overlook the request that has been sent before
                    if server.requests.get(&entry.name).is_none()
                        || server.requests.get(&entry.name).unwrap().seq < entry.seq
                    {
                        let mut reply = Reply {
                            seq: entry.seq,
                            value: String::new(),
                        };
                        my_debug3!(
                            "apply_msg op:{}, key:{}, value:{}",
                            entry.op,
                            entry.key,
                            entry.value
                        );
                        match entry.op {
                            0 => {
                                let ret = server.database.get(&entry.key);
                                match ret {
                                    Some(v) => {
                                        reply.value = v.to_string();
                                        my_debug3!(
                                            "successfully get! key: {}, value: {}",
                                            entry.key,
                                            v
                                        );
                                    }
                                    None => {}
                                }
                                server.requests.insert(entry.name.clone(), reply.clone());
                            }
                            1 => {
                                server
                                    .database
                                    .insert(entry.key.clone(), entry.value.clone());
                                server.requests.insert(entry.name.clone(), reply.clone());
                                my_debug3!(
                                    "successfully put, after that the value of key {} is {}",
                                    entry.key,
                                    server.database.get(&entry.key).unwrap()
                                );
                            }
                            2 => {
                                let ret = server.database.get(&entry.key);
                                match ret {
                                    Some(v) => {
                                        server
                                            .database
                                            .get_mut(&entry.key)
                                            .unwrap()
                                            .push_str(&entry.value.clone());
                                    }
                                    None => {
                                        server
                                            .database
                                            .insert(entry.key.clone(), entry.value.clone());
                                    }
                                }
                                my_debug3!(
                                    "successfully append, after that the value of key {} is {}",
                                    entry.key,
                                    server.database.get(&entry.key).unwrap()
                                );
                                server.requests.insert(entry.name.clone(), reply.clone());
                            }
                            _ => {}
                        }
                    }
                    server.snapshot_index = apply_msg.command_index;
                    server.save_snapshot();
                    if server.is_need_compress() {
                        let maxraftstate: usize = server.maxraftstate.unwrap();
                        server.rf.compress(maxraftstate, apply_msg.command_index);
                    }
                }
            }
        });
        *node.apply_thread.lock().unwrap() = Some(thread1);
    }

    /// the tester calls Kill() when a KVServer instance won't
    /// be needed again. you are not required to do anything
    /// in Kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    pub fn kill(&self) {
        // Your code here, if desired.
        self.server.lock().unwrap().rf.kill();
        *self.shutdown.lock().unwrap() = true;
        let apply_thread = self.apply_thread.lock().unwrap().take();
        if apply_thread.is_some() {
            let _ = apply_thread.unwrap().join();
        }
    }

    pub fn get_id(&self) -> usize {
        self.server.lock().unwrap().me
    }
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.get_state().term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.get_state().is_leader()
    }

    pub fn get_state(&self) -> raft::State {
        // Your code here.
        self.server.lock().unwrap().get_state()
    }
}

impl KvService for Node {
    fn get(&self, arg: GetRequest) -> RpcFuture<GetReply> {
        // Your code here.
        my_debug3!("server:{} get:{:?}", self.get_id(), arg);

        let mut reply = GetReply {
            wrong_leader: true,
            err: String::new(),
            value: String::new(),
        };
        if !self.is_leader() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        // lock server, we cannot lock it at the beginning, because is_leader()
        // will also ask for a lock
        let server = self.server.lock().unwrap();
        let ret = server.requests.get(&arg.name);
        match ret {
            Some(re) => {
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("");
                    return Box::new(futures::future::result(Ok(reply)));
                } else if arg.seq == re.seq {
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    reply.value = re.value.clone();
                    return Box::new(futures::future::result(Ok(reply)));
                } else {
                    let cmd = OpEntry {
                        seq: arg.seq,
                        name: arg.name.clone(),
                        op: 0,
                        key: arg.key.clone(),
                        value: String::new(),
                    };

                    let ret = server.rf.start(&cmd);
                    match ret {
                        Ok((_index, _term)) => {
                            reply.wrong_leader = false;
                            return Box::new(futures::future::result(Ok(reply)));
                        }
                        Err(_e) => {
                            reply.wrong_leader = true;
                            return Box::new(futures::future::result(Ok(reply)));
                        }
                    }
                }
            }
            None => {
                let cmd = OpEntry {
                    seq: arg.seq,
                    name: arg.name.clone(),
                    op: 0,
                    key: arg.key.clone(),
                    value: String::new(),
                };

                let ret = server.rf.start(&cmd);
                match ret {
                    Ok((_index, _term)) => {
                        reply.wrong_leader = false;
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    Err(_e) => {
                        reply.wrong_leader = true;
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                }
            }
        }
    }

    fn put_append(&self, arg: PutAppendRequest) -> RpcFuture<PutAppendReply> {
        // Your code here.
        my_debug3!("server:{} putappend:{:?}", self.get_id(), arg);
        let mut reply = PutAppendReply {
            wrong_leader: true,
            err: String::new(),
        };
        if !self.is_leader() {
            my_debug3!("error, server{} is not leader", self.get_id());
            return Box::new(futures::future::result(Ok(reply)));
        }
        let server = self.server.lock().unwrap();
        let ret = server.requests.get(&arg.name);
        match ret {
            Some(re) => {
                my_debug3!("successfully find");
                if arg.seq < re.seq {
                    reply.wrong_leader = true;
                    reply.err = String::from("OK");
                    return Box::new(futures::future::result(Ok(reply)));
                } else if arg.seq == re.seq {
                    reply.wrong_leader = false;
                    reply.err = String::from("OK");
                    return Box::new(futures::future::result(Ok(reply)));
                } else {
                    let cmd = OpEntry {
                        seq: arg.seq,
                        name: arg.name.clone(),
                        op: arg.op as u64,
                        key: arg.key.clone(),
                        value: arg.value.clone(),
                    };
                    let ret = server.rf.start(&cmd);
                    my_debug3!(
                        "put or append the value existed in requests via raft, cmd{:?}",
                        cmd
                    );
                    match ret {
                        Ok((_index, _term)) => {
                            reply.wrong_leader = false;
                            return Box::new(futures::future::result(Ok(reply)));
                        }
                        Err(_e) => {
                            reply.wrong_leader = true;
                            reply.err = String::from("");
                            return Box::new(futures::future::result(Ok(reply)));
                        }
                    }
                }
            }
            None => {
                my_debug3!("not found in requests");
                let cmd = OpEntry {
                    seq: arg.seq,
                    name: arg.name.clone(),
                    op: arg.op as u64,
                    key: arg.key.clone(),
                    value: arg.value.clone(),
                };
                my_debug3!(
                    "put or append the value not exist in requests via raft, cmd{:?}",
                    cmd
                );
                let ret = server.rf.start(&cmd);
                match ret {
                    Ok((_index, _term)) => {
                        reply.wrong_leader = false;
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                    Err(_e) => {
                        reply.wrong_leader = true;
                        reply.err = String::from("");
                        return Box::new(futures::future::result(Ok(reply)));
                    }
                }
            }
        }
    }
}
