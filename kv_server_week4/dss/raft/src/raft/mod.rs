// use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::sync::mpsc::Sender;
use std::sync::mpsc;
use std::time::Duration;
use std::cmp;
use rand::Rng;
// use crate::kvraft::service::kv::__futures::Future;

use futures::sync::mpsc::UnboundedSender;
use labrpc::RpcFuture;
use futures::Future;

#[cfg(test)]
pub mod config;
pub mod errors;
pub mod persister;
pub mod service;
#[cfg(test)]
mod tests;

use self::errors::*;
use self::persister::*;
use self::service::*;
// use crate::kvraft::server::OpEntry as Entry;
const TIMEOUT_LOWER_BOUND: u64 = 150;
const TIMEOUT_UPPER_BOUND: u64 = 300;
const APPEND_ENTRIES_INTERVAL: u64 = 50;
const MAX_SEND_ONCE: u64 = 500;

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => (
        println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    )
}
pub struct ApplyMsg {
    pub command_valid: bool,
    pub command: Vec<u8>,
    pub command_index: u64,
    pub is_snapshot: bool,
    pub data: Vec<u8>,
}

/// State of a raft peer.
#[derive(Default, Clone, Debug)]
pub struct State {
    pub term: u64,
    pub is_leader: bool,
    pub is_candidate: bool,
}

impl State {
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        self.term
    }
    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        self.is_leader
    }

    pub fn is_candidate(&self) -> bool {
        self.is_candidate
    }
    
}
#[derive(Clone, PartialEq, Message)]
pub struct RaftState {
    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(int64, tag = "2")]
    pub voted_for: i64,         

    #[prost(bytes, repeated, tag = "3")]
    pub logs: Vec<Vec<u8>>,

    #[prost(uint64, tag = "4")]
    pub snapshot_index: u64,

    #[prost(uint64, tag = "5")]
    pub snapshot_term: u64,
}
#[derive(Clone, PartialEq, Message)]
pub struct Entry {

    #[prost(uint64, tag = "1")]
    pub term: u64,

    #[prost(bytes, tag = "2")]
    pub entry: Vec<u8>,
}

impl Entry {
    pub fn new() -> Self {
        Entry {
            term: 0,
            entry: vec![],
        }
    }
    pub fn from_data(term2: u64, entry2: &Vec<u8>) -> Self {
        Entry {
            term: term2,
            entry: entry2.clone(),
        }
    }
}
// A single Raft peer.
pub struct Raft {
    // RPC end points of all peers
    peers: Vec<RaftClient>,
    // Object to hold this peer's persisted state
    persister: Box<dyn Persister>,
    // this peer's index into peers[]
    me: usize,
    state: Arc<State>,
    apply_ch: UnboundedSender<ApplyMsg>,
    // Your data here (2A, 2B, 2C).
    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.
    voted_for: Option<usize>,//投票给的节点的id,因为前面用usize存所以这里也用
    log: Vec<Entry>, //日志条目，从1开始
    commit_index: u64,//每个节点上已提交的最后一条日志条目索引
    last_applied: u64,//每个节点最后应用到状态机的日志条目索引
    //对于每个跟随者，领导者需要发送给他的下一条日志条目的索引，只有领导者需要维护
    next_index: Option<Vec<u64>>,
    //对于每个跟随者，领导者已经复制给他的最后一条日志条目的索引，只有领导者需要维护
    match_index: Option<Vec<u64>>,
    snapshot_index: u64,
    snapshot_term: u64,
}

impl Raft {
    /// the service or tester wants to create a Raft server. the ports
    /// of all the Raft servers (including this one) are in peers. this
    /// server's port is peers[me]. all the servers' peers arrays
    /// have the same order. persister is a place for this server to
    /// save its persistent state, and also initially holds the most
    /// recent saved state, if any. apply_ch is a channel on which the
    /// tester or service expects Raft to send ApplyMsg messages.
    /// This method must return quickly.
    pub fn new(
        peers: Vec<RaftClient>,
        me: usize,
        persister: Box<dyn Persister>,
        apply_ch: UnboundedSender<ApplyMsg>,
    ) -> Raft {
        let raft_state = persister.raft_state();

        // Your initialization code here (2A, 2B, 2C).
        let mut rf = Raft {
            peers,
            persister,
            me,
            state: Arc::default(),
            apply_ch,
            voted_for: None,
            log: vec![Entry::new()],//一开始往节点里塞一个空日志
            commit_index: 0,
            last_applied: 0,
            next_index: None,
            match_index: None,
            snapshot_index: 0,
            snapshot_term: 0,
        };

        // initialize from state persisted before a crash
        rf.restore(&raft_state);

        rf
    }

    /// save Raft's persistent state to stable storage,
    /// where it can later be retrieved after a crash and restart.
    /// see paper's Figure 2 for a description of what should be persistent.
    fn persist(&mut self) {
        // Your code here (2C).
        // Example:
        // labcodec::encode(&self.xxx, &mut data).unwrap();
        // labcodec::encode(&self.yyy, &mut data).unwrap();
        // self.persister.save_raft_state(data);
        let mut data = vec![];
        let mut voted_for: i64 = -1;
        if self.voted_for.is_some() {
            voted_for = self.voted_for.clone().unwrap() as i64;
        }
        let mut raft_state = RaftState {
            term: self.get_term(),
            voted_for: voted_for,
            logs: vec![],
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        };
        for i in 1..self.log.len() {
            //从1开始，不保存index为0的空log
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data).map_err(Error::Encode);
        self.persister.save_raft_state(data);
    }
    pub fn save_state_and_snapshot(&self, data: Vec<u8>) {
        let mut data2 = vec![];
        let mut voted_for: i64 = -1;
        if self.voted_for.is_some() {
            voted_for = self.voted_for.clone().unwrap() as i64;
        }
        let mut raft_state = RaftState {
            term: self.get_term(),
            voted_for: voted_for,
            logs: vec![],
            snapshot_index: self.snapshot_index,
            snapshot_term: self.snapshot_term,
        };
        for i in 1..self.log.len() {
            //从1开始，不保存index为0的空log
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data2).map_err(Error::Encode);
        self.persister.save_state_and_snapshot(data2, data);
    }
    /// restore previously persisted state.
    fn restore(&mut self, data: &[u8]) {
        if data.is_empty() {
            // bootstrap without any state?
            return;
        }
        // Your code here (2C).
        // Example:
        // match labcodec::decode(data) {
        //     Ok(o) => {
        //         self.xxx = o.xxx;
        //         self.yyy = o.yyy;
        //     }
        //     Err(e) => {
        //         panic!("{:?}", e);
        //     }
        // }
        match labcodec::decode(data) {
            Ok(o) => {
                let state: RaftState = o;
                let restate = Arc::new(State {
                    term: state.term,
                    is_leader: false,
                    is_candidate: false,
                });

                self.state = restate;
                self.commit_index = state.snapshot_index;
                self.last_applied = state.snapshot_index;
                self.snapshot_index = state.snapshot_index;
                self.snapshot_term = state.snapshot_term;
                self.log[0].term = self.snapshot_term;
                if state.voted_for == -1 {
                    self.voted_for = None;
                }
                else {
                    self.voted_for = Some(state.voted_for as usize);
                }

                for i in 0..state.logs.len() {
                    let log_encode = state.logs[i].clone();
                    match labcodec::decode(&log_encode) {
                        Ok(log) => {
                            let log: Entry = log;
                            self.log.push(log.clone());
                            my_debug!("id:{} restore log :{:?}", self.me, log);
                        }
                        Err(e) => {
                            panic!("{:?}", e);
                        }
                    }
                }
            }
            Err(e) => {
                panic!("{:?}", e);
            }
        }
    }

    /// example code to send a RequestVote RPC to a server.
    /// server is the index of the target server in peers.
    /// expects RPC arguments in args.
    ///
    /// The labrpc package simulates a lossy network, in which servers
    /// may be unreachable, and in which requests and replies may be lost.
    /// This method sends a request and waits for a reply. If a reply arrives
    /// within a timeout interval, This method returns Ok(_); otherwise
    /// this method returns Err(_). Thus this method may not return for a while.
    /// An Err(_) return can be caused by a dead server, a live server that
    /// can't be reached, a lost request, or a lost reply.
    ///
    /// This method is guaranteed to return (perhaps after a delay) *except* if
    /// the handler function on the server side does not return.  Thus there
    /// is no need to implement your own timeouts around this method.
    ///
    /// look at the comments in ../labrpc/src/mod.rs for more details.
    fn send_request_vote(&self, server: usize, args: &RequestVoteArgs) -> Result<RequestVoteReply> {
        let peer = &self.peers[server];
        // Your code here if you want the rpc becomes async.
        // Example:
        // ```
        // let (tx, rx) = channel();
        // peer.spawn(
        //     peer.request_vote(&args)
        //         .map_err(Error::Rpc)
        //         .then(move |res| {
        //             tx.send(res);
        //             Ok(())
        //         }),
        // );
        // rx.wait() ...
        // ```
        peer.request_vote(&args).map_err(Error::Rpc).wait()
    }
    //测试框架中客户端会调用此函数来向节点发送日志
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        let index = self.snapshot_index + self.log.len() as u64;
        let term = self.get_term();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        //如果是领导人，那么将这条日志添加到领导人日志条目中
        if self.is_leader() {
            self.append_log(term, &buf);
            Ok((index, term))
        } else {//如果不是，那么返回，因为跟随者不能从客户端接受日志
            Err(Error::NotLeader)
        }
    }
    pub fn get_peers_amount(&self) -> usize {
        self.peers.len()
    }

    pub fn get_term(&self) -> u64 {
        self.state.term()
    }

    pub fn is_leader(&self) -> bool {
        self.state.is_leader()
    }

    pub fn is_candidate(&self) -> bool {
        self.state.is_candidate()
    }

    pub fn get_last_log_index(&self) -> usize {
        self.snapshot_index as usize + self.log.len() - 1
    }

    pub fn get_last_log_term(&self) -> u64 {
        let index = self.get_last_log_index();
        self.log[index - self.snapshot_index as usize].term
    }
    pub fn set_state(&mut self, term: u64, is_leader: bool, is_candidate: bool) {
        let state = State {
            term,
            is_leader,
            is_candidate,
        };
        self.state = Arc::new(state);
        if self.is_candidate() {
            self.voted_for = Some(self.me);
            self.next_index = None;
            self.match_index = None;
        }
        else if self.is_leader() {
            self.next_index = Some(vec![self.snapshot_index + 1; self.peers.len()]);
            self.match_index = Some(vec![0; self.peers.len()]);
        }
        else {
            self.voted_for = None;
            self.match_index = None;
            self.next_index = None;
        }
        self.persist();
    }
    pub fn get_log(&self, index: usize) -> Option<Entry> {
        let index = index - self.snapshot_index as usize;
        if self.log.len() - 1 < index {
            None
        }
        else {
            Some(self.log[index].clone())
        }
    }
    //在节点所有日志末尾再添加一条
    pub fn append_log(&mut self, term: u64, entry: &Vec<u8>) {
        self.log.push(Entry::from_data(term, entry));
        self.persist();
    }
    //将节点中第index条之后（不包括index本身）的日志全部删除
    pub fn delete_log(&mut self, index: usize) {
        let index = index - self.snapshot_index as usize;
        if self.log.len() - 1 < index {
            return;
        }
        let _delete: Vec<Entry> = self.log.drain((index + 1)..).collect();
        self.persist();
    }
    //将节点中第index条之前（不包括index本身）的日志全部删除
    pub fn delete_prev_log(&mut self, index: usize) {
        let index = index  - self.snapshot_index as usize;
        if self.log.len() - 1 < index {
            return;
        }
        let _delete: Vec<Entry> = self.log.drain(..(index as usize)).collect();
        self.persist();
    }
    pub fn handle_success_reply(&mut self, id: usize, for_next_index: u64) {
        if !self.is_leader() || for_next_index < 1 {
            return;
        }
        
        let mut match_index = self.match_index.clone().unwrap();
        let mut next_index = self.next_index.clone().unwrap();
        let old_match_index = match_index[id];
        let old_next_index = next_index[id];
        if for_next_index <= self.snapshot_index + self.log.len() as u64 {
            match_index[id] = for_next_index - 1;
            next_index[id] = for_next_index;
        }
        else {
            my_debug!("error:leader:{} handle_append_reply id:{} for_next_index:{} log:{} ",
                self.me, id, for_next_index, self.log.len());
            return;
        }
        self.next_index = Some(next_index.clone());
        self.match_index = Some(match_index.clone());
        if old_match_index == match_index[id] {
            return;
        }
        //查看是否更新commit
        let mut new_commit_index: u64 = 0;
        for index in ((self.commit_index + 1)..(match_index[id] + 1)).rev() {
            //rev()逆序，从大到小开始检测
            let mut pass: usize = 0;
            for i in 0..self.get_peers_amount() {
                if i == self.me {
                    continue;
                }
                if match_index[i] >= index {
                    pass += 1;
                }
            }
            if (pass + 1) > self.get_peers_amount() / 2 {
                //说明通过超过半数，可commit
                new_commit_index = index;
                break;
            }
        }
        if new_commit_index != 0 {
            let log = self.get_log(new_commit_index as usize).unwrap();
            if log.term != self.get_term() {
                return;
            }
            //my_debug!("id:{} new_commit_index:{:?}", self.me, new_commit_index);
            self.set_commit_index(new_commit_index);
        }
    }
    pub fn handle_fail_reply(&mut self, id: usize, fail_type: u64, for_next_index: u64, conflict_term: u64, earlist_conflict_index: u64) {
        if !self.is_leader() {
            return;
        }
        let log_index = self.snapshot_index + self.log.len() as u64;
        let mut next_index = self.next_index.clone().unwrap();
        let mut can_find: bool = false;
        if (for_next_index > self.snapshot_index + 1) && for_next_index <= log_index {  
                next_index[id] = for_next_index;
            }
        else if fail_type == 0 {
            for i in (self.snapshot_index as usize + 1)..(self.snapshot_index as usize + self.log.len() - 1) {
                let entry = self.get_log(i);
                match entry {
                    Some(en) => {
                        if en.term == conflict_term {
                            next_index[id] = i as u64;
                            can_find = true;
                        }
                    },
                    None => {}
                }
            }
            if can_find == false {
                next_index[id] = earlist_conflict_index; 
            }
        }
        else {
            if next_index[id] < (MAX_SEND_ONCE + 1) {
                next_index[id] = 1;
            }
            else {
                next_index[id] -= MAX_SEND_ONCE;
            }
        }
        self.next_index = Some(next_index.clone());
        // let mut next_index = self.next_index.clone().unwrap();
        // let mut can_find: bool = false;
        // if fail_type == 0 {
        //     for i in (self.snapshot_index as usize + 1)..(self.snapshot_index as usize + self.log.len() - 1) {
        //         let entry = self.get_log(i);
        //         match entry {
        //             Some(en) => {
        //                 if en.term == conflict_term {
        //                     next_index[id] = i as u64;
        //                     can_find = true;
        //                 }
        //             },
        //             None => {}
        //         }
        //     }
        //     if can_find == false {
        //         next_index[id] = earlist_conflict_index; 
        //     }
        // }
        // else {
        //     next_index[id] = for_next_index;
        // }
        // if (for_next_index > self.snapshot_index + 1) && for_next_index <= log_index {  
        //     //snapshot时，follower希望的next_index有效，则可设置next_index，
        //         next_index[id] = for_next_index;
        //     }
        //     else {
        //         if next_index[id] < (MAX_SEND_ONCE + 1) {
        //             next_index[id] = 1;
        //         }
        //         else {
        //             next_index[id] -= MAX_SEND_ONCE;
        //         }
        //     }
        // self.next_index = Some(next_index.clone());
    }
    pub fn set_commit_index(&mut self, new_commit_index: u64) {
        if new_commit_index < self.commit_index as u64 {
            my_debug!(
                "error:id:{} set_commit_index fail:[{}-{}]",
                self.me,
                self.commit_index,
                new_commit_index
            );
            return;
        }
        my_debug!(
            "id:{} set commit_index:[{}->{}]",
            self.me,
            self.commit_index,
            new_commit_index
        );
        self.commit_index = new_commit_index ;
        if self.commit_index > self.last_applied {
            //更新状态机
            let last = self.last_applied;
            for i in last..self.commit_index {
                self.last_applied += 1; //并发送
                let mesg = ApplyMsg {
                    command_valid: true,
                    command: self.log[(self.last_applied - self.snapshot_index) as usize].entry.clone(),
                    command_index: self.last_applied,
                    is_snapshot: false,
                    data: vec![],
                };
                let _ret = self.apply_ch.unbounded_send(mesg);
                my_debug!("id:{} apply_ch:[{}]", self.me, self.last_applied);
                self.persist();
            }
        }
    }
    pub fn compress(&mut self, maxraftstate: usize, index: u64) {
        if maxraftstate > self.persister.raft_state().len() {  //未超过，无需压缩日志
            return;
        }
        if index > self.commit_index || index <= self.snapshot_index {
            return;
        }
        self.delete_prev_log(index as usize);  //删除index之前的日志
        self.snapshot_index = index;
        self.snapshot_term = self.log[0].term;
        self.persist();  

    }
    
}

// Choose concurrency paradigm.
//
// You can either drive the raft state machine by the rpc framework,
//
// ```rust
// struct Node { raft: Arc<Mutex<Raft>> }
// ```
//
// or spawn a new thread runs the raft state machine and communicate via
// a channel.
//
// ```rust
// struct Node { sender: Sender<Msg> }
// ```
#[derive(Clone)]
pub struct Node {
    // Your code here.
    shutdown: Arc<Mutex<bool>>,
    raft: Arc<Mutex<Raft>>,
    timeout_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
    timeout_reset: Arc<Mutex<Option<Sender<u64>>>>,
    append_entries_thread: Arc<Mutex<Option<thread::JoinHandle<()>>>>,
}

impl Node {
    /// Create a new raft service.
    pub fn new(raft: Raft) -> Node {
        // Your code here.
        let node = Node {
            shutdown: Arc::new(Mutex::new(false)),
            raft: Arc::new(Mutex::new(raft)),
            timeout_thread: Arc::new(Mutex::new(None)),
            timeout_reset: Arc::new(Mutex::new(None)),
            append_entries_thread: Arc::new(Mutex::new(None)),
        };
        Node::create_timeout_thread(node.clone());
        my_debug!("New inode:{}", node.raft.lock().unwrap().me);
        node
    }
    pub fn create_timeout_thread(node: Node) {
        let (timeout_reset, recv) = mpsc::channel();
        let node2 = node.clone();
        let thread1 = thread::spawn(move || {
            loop {
                //设置随机超时时间
                let rand_time = Duration::from_millis(rand::thread_rng().gen_range(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND));
                //如果未超时，继续
                if let Ok(_) = recv.recv_timeout(rand_time) {
                    if *node2.shutdown.lock().unwrap() == true {  //关闭
                        break;
                    }
                    continue;
                }
                else {
                    if *node2.shutdown.lock().unwrap() == true {  //关闭
                        break;
                    }
                    //超时了但是自己是领导者，没关系
                    if node2.is_leader() {
                        continue;
                    }
                    //不是领导者那么要开始投票请求
                    Node::do_vote(node2.clone());
                }
            }
        });
        *node.timeout_thread.lock().unwrap() = Some(thread1);
        *node.timeout_reset.lock().unwrap() = Some(timeout_reset);
    }

    pub fn do_vote(node: Node) {
        let mut raft = node.raft.lock().unwrap(); //锁住raft
        let mut current_term = raft.get_term();
        current_term += 1;
        let id = raft.me;
        raft.set_state(current_term, false, true);
        let last_log_index2 = raft.get_last_log_index() as u64;
        let last_log_term2 = raft.get_last_log_term();
        let args = RequestVoteArgs {
            term: current_term,
            candidate_id: id as u64,
            last_log_index: last_log_index2,
            last_log_term: last_log_term2,
        };
        my_debug!(
            "candidate:{} term:{}  args:{:?}",
            raft.me,
            raft.get_term(),
            args
        );
        let amount = raft.get_peers_amount();
        let passed = Arc::new(Mutex::new(1));
        let peers = raft.peers.clone();
        for i in 0..amount {
            if i == id {
                continue;
            }
            let node2 = node.clone();
            let passed = Arc::clone(&passed);
            let peer = peers[i].clone();
            let term = raft.get_term().clone();
            let args2 = args.clone();
            let amount2 = amount as u64;
            thread::spawn(move || {
                let ret = peer.request_vote(&args2).map_err(Error::Rpc).wait();
                match ret {
                    Ok(t) => {
                        let mut raft = node2.raft.lock().unwrap();
                        if t.vote_granted {
                            *passed.lock().unwrap() += 1;
                            if *passed.lock().unwrap() > amount2 / 2 && term == raft.get_term() && raft.is_candidate() {
                                raft.set_state(term, true, false);
                                my_debug!("{} become leader!", raft.me);
                                let node3 = node2.clone();
                                thread::spawn(move || {
                                    Node::do_append_thread(node3);
                                });
                            }
                        } else if t.term > raft.get_term() {
                            let _ret = node2.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                            raft.set_state(t.term, false, false);
                        }
                    },
                    Err(_e) => {

                    }
                }
            });
            
        }
    }

    pub fn do_append_thread(node: Node) {
        let node2 = node.clone();
        let thread1 = thread::spawn(move || {
            loop {
                let interval = Duration::from_millis(APPEND_ENTRIES_INTERVAL);
                thread::sleep(interval);
                if *node2.shutdown.lock().unwrap() == true {  //关闭
                    break;
                }
                if node2.is_leader() {
                    Node::do_append_entries(node2.clone());
                }
                else {continue;}
            }
        });
        *node.append_entries_thread.lock().unwrap() = Some(thread1);
    }

    pub fn do_append_entries(node: Node) {
        let raft = node.raft.lock().unwrap();
        let id = raft.me;
        let amount = raft.get_peers_amount();
        let next_index = match raft.next_index.clone() {
            Some(index) => index.clone(),
            None => {
                return;
            }
        };
        my_debug!("leader:{} heart beat! term:{}", id, raft.get_term());
        my_debug!("next_index: {:?}", next_index);
        for i in 0..amount {
            if i == id {
                continue;
            }
            let peer = raft.peers[i].clone();
            let node2 = node.clone();
            let prev_log_index2 = next_index[i] - 1;
            if prev_log_index2 < raft.snapshot_index {//给太落后的跟随者发送快照
                //简化了，只发送一次快照
                let args = RequestSnapshotArgs {
                    term: raft.get_term(),
                    leader_id: id as u64,
                    last_included_index: raft.snapshot_index,
                    last_included_term: raft.snapshot_term,
                    data: raft.persister.snapshot(),
                };
                thread::spawn(move || {
                    let ret = peer.install_snapshot(&args).map_err(Error::Rpc).wait();
                    match ret {
                        Ok(o) => {
                            let mut raft = node2.raft.lock().unwrap();
                            if o.term > raft.get_term() {
                                let _ret = node2.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                                raft.set_state(o.term, false, false);
                            }
                            if raft.is_leader() {
                                let mut next_index = raft.next_index.clone().unwrap();
                                let mut match_index = raft.match_index.clone().unwrap();
                                next_index[i] = args.last_included_index + 1;
                                match_index[i] = args.last_included_index;
                                raft.next_index = Some(next_index.clone());
                                raft.match_index = Some(match_index.clone());
                            }
                        },
                        Err(_e) => {}
                    }
                });
            }
            else {
                let mut args = RequestEntryArgs {
                    term: raft.get_term(),
                    leader_id: id as u64,
                    prev_log_index: prev_log_index2,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: raft.commit_index
                };
                let entry = raft.get_log(args.prev_log_index as usize); //获取log
                args.prev_log_term = entry.unwrap().term;
                for j in 0..MAX_SEND_ONCE {  //一次发送至多MAX_SEND_ONCE个log
                    let entry_next = raft.get_log((next_index[i] + j) as usize);
                    match entry_next {
                        Some(en) => {
                            let mut dat = vec![];
                            let _ret = labcodec::encode(&en, &mut dat).map_err(Error::Encode);
                            args.entries.push(dat);
                        }
                        None => {
                            break;
                        }
                    }
                }
                thread::spawn(move || {
                    my_debug!("leader:{} do heart beat:{} args:[term:{} prev_index:{} prev_term:{} commit:{} entry_num:{}] ", 
                    id, i, args.term, args.prev_log_index, args.prev_log_term, args.leader_commit, args.entries.len());
                    let ret = peer.append_entries(&args).map_err(Error::Rpc).wait();
                    match ret {
                        Ok(t) => {
                            let mut raft = node2.raft.lock().unwrap();
                            if t.success {
                                if t.term == raft.get_term() {
                                    raft.handle_success_reply(i, t.next_index);
                                }
                                
                            }
                            else {
                                if t.term > raft.get_term() {
                                    let _ret = node2.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                                    raft.set_state(t.term, false, false);
                                    my_debug!("leader {} become follower because it meets a bigger term {} from {}", id, t.term, i);
                                    
                                }
                                else {
                                    if t.term == raft.get_term() {
                                        raft.handle_fail_reply(i, t.fail_type, t.next_index, t.conflict_term, t.earlist_conflict_index);
                                    }
                                    
                                }
                            }
                        },
                        Err(_e) => {},
                    }
                });
            }
        }
    }
    /// the service using Raft (e.g. a k/v server) wants to start
    /// agreement on the next command to be appended to Raft's log. if this
    /// server isn't the leader, returns [`Error::NotLeader`]. otherwise start
    /// the agreement and return immediately. there is no guarantee that this
    /// command will ever be committed to the Raft log, since the leader
    /// may fail or lose an election. even if the Raft instance has been killed,
    /// this function should return gracefully.
    ///
    /// the first value of the tuple is the index that the command will appear
    /// at if it's ever committed. the second is the current term.
    ///
    /// This method must return without blocking on the raft.
    pub fn start<M>(&self, command: &M) -> Result<(u64, u64)>
    where
        M: labcodec::Message,
    {
        // Your code here.
        // Example:
        // self.raft.start(command)
        if self.is_leader() {
            self.raft.lock().unwrap().start(command)
        }
        else {
            Err(Error::NotLeader)
        }
    }
    /// The current term of this peer.
    pub fn term(&self) -> u64 {
        // Your code here.
        // Example:
        // self.raft.term
        // unimplemented!()
        self.raft.lock().unwrap().get_term()
    }

    /// Whether this peer believes it is the leader.
    pub fn is_leader(&self) -> bool {
        // Your code here.
        // Example:
        // self.raft.leader_id == self.id
        // unimplemented!()
        self.raft.lock().unwrap().is_leader()
    }
    pub fn is_candidate(&self) -> bool {
        self.raft.lock().unwrap().is_candidate()
    }
    /// The current state of this peer.
    pub fn get_state(&self) -> State {
        State {
            term: self.term(),
            is_leader: self.is_leader(),
            is_candidate: self.is_candidate()
        }
    }
    pub fn save_snapshot(&self, data: Vec<u8>) {
        self.raft.lock().unwrap().save_state_and_snapshot(data);
    }
    pub fn compress(&self, maxraftstate: usize, index: u64) {
        self.raft.lock().unwrap().compress(maxraftstate, index);
    }
    /// the tester calls kill() when a Raft instance won't be
    /// needed again. you are not required to do anything in
    /// kill(), but it might be convenient to (for example)
    /// turn off debug output from this instance.
    /// In Raft paper, a server crash is a PHYSICAL crash,
    /// A.K.A all resources are reset. But we are simulating
    /// a VIRTUAL crash in tester, so take care of background
    /// threads you generated with this Raft Node.
    pub fn kill(&self) {
        // Your code here, if desired.
        *self.shutdown.lock().unwrap() = true;
        
        match *self.timeout_reset.lock().unwrap() {
            Some(ref send) => {
                let _ret = send.send(1);
            },
            None => {}
        }
        let timeout_thread = self.timeout_thread.lock().unwrap().take();
        if timeout_thread.is_some() {
            let _ = timeout_thread.unwrap().join();
        }
        let append_entries_thread = self.append_entries_thread.lock().unwrap().take();
        if append_entries_thread.is_some() {
            let _ = append_entries_thread.unwrap().join();
        }
    }
}

impl RaftService for Node {
    // example RequestVote RPC handler.
    fn request_vote(&self, args: RequestVoteArgs) -> RpcFuture<RequestVoteReply> {
        // Your code here (2A, 2B).
        // unimplemented!()
        let mut raft = self.raft.lock().unwrap();
        let mut reply = RequestVoteReply {
            term: raft.get_term(),
            vote_granted: false,
        };
        if args.term < reply.term {
            return Box::new(futures::future::result(Ok(reply)));
        }
        if args.term > reply.term {
            let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
            raft.set_state(args.term, false, false);
        }
        match raft.voted_for {
            Some(_) => {},
            None => {
                let last_log_index = raft.get_last_log_index() as u64;
                let last_log_term = raft.get_last_log_term();
                //选举限制
                if (last_log_term == args.last_log_term && last_log_index <= args.last_log_index) ||
                last_log_term < args.last_log_term {
                    reply.vote_granted = true;
                    raft.voted_for = Some(args.candidate_id as usize);
                    raft.persist();
                    my_debug!("node {} vote for node {}", raft.me, args.candidate_id);
                }
            }
        }
        Box::new(futures::future::result(Ok(reply)))
    }
    fn append_entries(&self, args: RequestEntryArgs) -> RpcFuture<RequestEntryReply> {
        let mut raft = self.raft.lock().unwrap();
        my_debug!("node {} receive the entry from leader {}, node.term: {}, args.term: {}", raft.me, args.leader_id, raft.get_term(), args.term);
        let mut reply = RequestEntryReply {
            term: raft.get_term(),
            success: false,
            next_index: raft.snapshot_index + 1,
            conflict_term: 0,
            earlist_conflict_index: raft.snapshot_index + 1,
            fail_type: 0,
        };
        if args.term < raft.get_term() {
            return Box::new(futures::future::result(Ok(reply)));//任期错误时，返回目标任期，以告知领导者修改错误
        }
        let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
        raft.set_state(args.term, false, false);
        raft.voted_for = Some(args.leader_id as usize);
        raft.persist();
        reply.term = args.term;//返回自己的任期
        if args.prev_log_index < raft.snapshot_index {
            return Box::new(futures::future::result(Ok(reply)));
        }
        let entry = raft.get_log(args.prev_log_index as usize);
        match entry {
            Some(en) => {
                if en.term == args.prev_log_term {//日志匹配
                    if args.entries.len() != 0 {
                        for i in 0..args.entries.len() {
                            let log_encode = args.entries[i].clone();
                            match labcodec::decode(&log_encode) {
                                Ok(log) => {
                                    let log: Entry = log;
                                    let node_entry = raft.get_log(args.prev_log_index as usize + 1 + i);
                                    //一致性检查
                                    match node_entry {
                                        Some(n_en) => {
                                            if n_en == log {  //log 相等，无需删除
                                                continue;
                                            } else {//不相等时，把跟随者冲突的日志条目全部删除并且加上领导人的日志
                                                raft.delete_log(args.prev_log_index as usize + i);
                                                raft.append_log(log.term, &log.entry);
                                            }
                                        }
                                        None => {
                                            raft.append_log(log.term, &log.entry);
                                        }
                                    }
                                }
                                Err(e) => {
                                    panic!("{:?}", e);
                                }
                            }
                        }
                    }
                    reply.success = true;
                    reply.next_index = args.prev_log_index + 1 + args.entries.len() as u64;
                    let can_commit = cmp::min(args.leader_commit, reply.next_index - 1);
                    if can_commit > raft.commit_index as u64 {
                        let last_log_index = raft.get_last_log_index();
                        let new_commit_index: u64 = cmp::min(can_commit, last_log_index as u64);
                        raft.set_commit_index(new_commit_index);
                    }
                    let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                }
                //日志不匹配时，找到跟随者冲突任期中最早的那条日志的索引
                else {
                    reply.conflict_term = en.term;
                    for i in (raft.snapshot_index + raft.log.len() as u64 - 1)..(raft.snapshot_index + 1) {
                        let entry = raft.get_log(i as usize);
                        match entry {
                            Some(en) => {
                                if en.term == reply.conflict_term {
                                    reply.earlist_conflict_index = i;
                                }
                            },
                            None => {},
                        }
                    }
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            None => {
                // reply.conflict_term = raft.get_last_log_term();
                // for i in (raft.log.len() - 1)..0 {
                //     let entry = raft.get_log(i);
                //     match entry {
                //         Some(en) => {
                //             if en.term == reply.conflict_term {
                //                 reply.earlist_conflict_index = i as u64;
                //             }
                //         },
                //         None => {},
                //     }
                // }
                // reply.next_index = raft.snapshot_index + 
                // raft.log.len() as u64;
                reply.fail_type = 1;
                return Box::new(futures::future::result(Ok(reply)));
            }
        }
        Box::new(futures::future::result(Ok(reply)))
    }

    fn install_snapshot(&self, args: RequestSnapshotArgs) -> RpcFuture<RequestSnapshotReply> {
        let mut raft = self.raft.lock().unwrap();
        let mut reply = RequestSnapshotReply {
            term: raft.get_term(),
        };
        if args.term < raft.get_term() {
            return Box::new(futures::future::result(Ok(reply)));
        }
        if args.last_included_index <= raft.snapshot_index {
		    my_debug!("warn:me[{}:{}] recv snapshot [{}:{}]", raft.me, raft.snapshot_index, args.leader_id, args.last_included_index);
		    return Box::new(futures::future::result(Ok(reply)));
	    }
        let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
        if args.term == raft.get_term() && raft.is_candidate() {  //candidate遇到leader
            raft.set_state(args.term, false, false);
            raft.voted_for = Some(args.leader_id as usize);
            raft.persist();
        }
        if args.term > raft.get_term() {  //遇到term更大，变成follower，无论是candidate还是leader，都变为follower
            raft.set_state(args.term, false, false);
            raft.voted_for = Some(args.leader_id as usize);
            raft.persist();
        }
        reply.term = raft.get_term();
        
        if args.last_included_index > (raft.snapshot_index + raft.log.len() as u64 - 1) { //所有log都要替换
            let log = Entry {
                term: args.last_included_term,
                entry: vec![],
            };
            raft.log = vec![log];
        }
        else {
            raft.delete_prev_log(args.last_included_index as usize, );
        }
        raft.snapshot_index = args.last_included_index;
        raft.snapshot_term = args.last_included_term;
        raft.commit_index = args.last_included_index;
        raft.last_applied = args.last_included_index;

        raft.save_state_and_snapshot(args.data.clone());  //保存state和snapshot

        let mesg = ApplyMsg {
            command_valid: true,
            command: vec![],
            command_index: raft.snapshot_index,
            is_snapshot: true,
            data: args.data.clone(),
        };
        let _ret = raft.apply_ch.unbounded_send(mesg);  //发送snapshot给kv_server

        Box::new(futures::future::result(Ok(reply)))
    }
}
