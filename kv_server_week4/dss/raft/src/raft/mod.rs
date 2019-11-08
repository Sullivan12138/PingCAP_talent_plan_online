use rand::Rng;
use std::cmp;
use std::sync::mpsc;
use std::sync::mpsc::Sender;
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::Duration;

use futures::sync::mpsc::UnboundedSender;
use futures::Future;
use labrpc::RpcFuture;

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
const TIMEOUT_LOWER_BOUND: u64 = 150;
const TIMEOUT_UPPER_BOUND: u64 = 300;
const APPEND_ENTRIES_INTERVAL: u64 = 50;
const MAX_SEND_ONCE: u64 = 500;

#[macro_export]
macro_rules! my_debug {
    ($($arg: tt)*) => {
        // println!("Debug[{}:{}]: {}", file!(), line!(),format_args!($($arg)*));
    };
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
    /// Wheteher this peer believes it is the candidate.
    pub fn is_candidate(&self) -> bool {
        self.is_candidate
    }
}
/// the state a node shall save when persisting
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
/// the log structure
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
    // the id this peer has voted for in current term
    voted_for: Option<usize>,
    // the logs saved in this peer, start from 1
    log: Vec<Entry>,
    // the latest index of the logs that this peer has committed
    commit_index: u64,
    // the latest index of the logs that this peer has applied to state machine
    last_applied: u64,
    // the index of the log that the leader should send to each follower next time,
    // only maintained by leader
    next_index: Option<Vec<u64>>,
    // the index of the latest log that the leader has already copied to each follower,
    // only maintained by leader
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
            log: vec![Entry::new()], // push an empty log into the peer initially
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
            // do not need to save the log of index 0
            let mut dat = vec![];
            let log = self.log[i].clone();
            let _ret = labcodec::encode(&log, &mut dat).map_err(Error::Encode);
            raft_state.logs.push(dat);
        }
        let _ret = labcodec::encode(&raft_state, &mut data).map_err(Error::Encode);
        self.persister.save_raft_state(data);
    }
    /// save snapshot
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
            // do not need to save the log of index 02
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
                } else {
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
    // int test.rs client will call this method to send logs to nodes
    fn start<M>(&mut self, command: &M) -> Result<(u64, u64)>
        where
            M: labcodec::Message,
    {
        let index = self.snapshot_index + self.log.len() as u64;
        let term = self.get_term();
        let mut buf = vec![];
        labcodec::encode(command, &mut buf).map_err(Error::Encode)?;
        // Your code here (2B).
        // if this node is a leader, append the log into leader's logs
        if self.is_leader() {
            self.append_log(term, &buf);
            Ok((index, term))
        } else {
            // if not, return. Because only leader can accept logs from client.
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
        } else if self.is_leader() {
            self.next_index = Some(vec![self.snapshot_index + 1; self.peers.len()]);
            self.match_index = Some(vec![0; self.peers.len()]);
        } else {
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
        } else {
            Some(self.log[index].clone())
        }
    }
    // append a log after all the logs in the node
    pub fn append_log(&mut self, term: u64, entry: &Vec<u8>) {
        self.log.push(Entry::from_data(term, entry));
        self.persist();
    }
    // delete all the logs after the log in index position(not include the log in index position itself)
    pub fn delete_log(&mut self, index: usize) {
        let index = index - self.snapshot_index as usize;
        if self.log.len() - 1 < index {
            return;
        }
        let _delete: Vec<Entry> = self.log.drain((index + 1)..).collect();
        self.persist();
    }
    // delete all the logs before the log in index position(not include the log in index position itself)
    pub fn delete_prev_log(&mut self, index: usize) {
        let index = index - self.snapshot_index as usize;
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
        } else {
            my_debug!(
                "error:leader:{} handle_append_reply id:{} for_next_index:{} log:{} ",
                self.me,
                id,
                for_next_index,
                self.log.len()
            );
            return;
        }
        self.next_index = Some(next_index.clone());
        self.match_index = Some(match_index.clone());
        if old_match_index == match_index[id] {
            return;
        }
        // check whether to update commit_index
        let mut new_commit_index: u64 = 0;
        for index in ((self.commit_index + 1)..(match_index[id] + 1)).rev() {
            // check it from big to small
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
                // if copy numbers > half of the amount, commit
                new_commit_index = index;
                break;
            }
        }
        if new_commit_index != 0 {
            let log = self.get_log(new_commit_index as usize).unwrap();
            if log.term != self.get_term() {
                return;
            }
            self.set_commit_index(new_commit_index);
        }
    }
    pub fn handle_fail_reply(
        &mut self,
        id: usize,
        fail_type: u64,
        for_next_index: u64,
        conflict_term: u64,
        earlist_conflict_index: u64,
    ) {
        if !self.is_leader() {
            return;
        }
        let log_index = self.snapshot_index + self.log.len() as u64;
        let mut next_index = self.next_index.clone().unwrap();
        let mut can_find: bool = false;
        if (for_next_index > self.snapshot_index + 1) && for_next_index <= log_index {
            next_index[id] = for_next_index;
        } else if fail_type == 0 {
            for i in (self.snapshot_index as usize + 1)
                ..(self.snapshot_index as usize + self.log.len() - 1)
                {
                    let entry = self.get_log(i);
                    match entry {
                        Some(en) => {
                            if en.term == conflict_term {
                                next_index[id] = i as u64;
                                can_find = true;
                            }
                        }
                        None => {}
                    }
                }
            if can_find == false {
                next_index[id] = earlist_conflict_index;
            }
        } else {
            if next_index[id] < (MAX_SEND_ONCE + 1) {
                next_index[id] = 1;
            } else {
                next_index[id] -= MAX_SEND_ONCE;
            }
        }
        self.next_index = Some(next_index.clone());
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
        self.commit_index = new_commit_index;
        if self.commit_index > self.last_applied {
            // update state machine
            let last = self.last_applied;
            for i in last..self.commit_index {
                self.last_applied += 1;
                // send to the client
                let mesg = ApplyMsg {
                    command_valid: true,
                    command: self.log[(self.last_applied - self.snapshot_index) as usize]
                        .entry
                        .clone(),
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
        if maxraftstate > self.persister.raft_state().len() {
            // not exceed, no need to compress
            return;
        }
        if index > self.commit_index || index <= self.snapshot_index {
            return;
        }
        // delete the logs before index
        self.delete_prev_log(index as usize);
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
    append_entries_reset: Arc<Mutex<Option<Sender<u64>>>>,
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
            append_entries_reset: Arc::new(Mutex::new(None)),
        };
        node.create_timeout_thread();
        node.create_append_entries_thread();
        my_debug!("New inode:{}", node.raft.lock().unwrap().me);
        node
    }
    pub fn create_timeout_thread(&self) {
        let (timeout_reset, recv) = mpsc::channel();
        let node = self.clone();
        let thread1 = thread::spawn(move || {
            loop {
                // set a rand timeout
                let rand_time = Duration::from_millis(
                    rand::thread_rng().gen_range(TIMEOUT_LOWER_BOUND, TIMEOUT_UPPER_BOUND),
                );
                // if time doesn't exceed, continue
                if let Ok(_) = recv.recv_timeout(rand_time) {
                    if *node.shutdown.lock().unwrap() == true {
                        break;
                    }
                    continue;
                } else {
                    if *node.shutdown.lock().unwrap() == true {
                        break;
                    }
                    // if time exceeds but itself has already become leader, conntinue
                    if node.is_leader() {
                        continue;
                    }
                    // otherwise send request_vote
                    node.clone().do_vote();
                }
            }
        });
        *self.timeout_thread.lock().unwrap() = Some(thread1);
        *self.timeout_reset.lock().unwrap() = Some(timeout_reset);
    }

    pub fn do_vote(&self) {
        let mut raft = self.raft.lock().unwrap(); // lock
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
            let node = self.clone();
            let passed = Arc::clone(&passed);
            let peer = peers[i].clone();
            let term = raft.get_term().clone();
            let args2 = args.clone();
            let amount2 = amount as u64;
            peer.spawn(
               peer.request_vote(&args2).map(move |ret| {
                   let mut raft = node.raft.lock().unwrap();
                   if ret.vote_granted {
                       *passed.lock().unwrap() += 1;
                       if *passed.lock().unwrap() > amount2 / 2
                           && term == raft.get_term()
                           && raft.is_candidate()
                       {
                           raft.set_state(term, true, false);
                           my_debug!("{} become leader!", raft.me);
                           let _ret = node.append_entries_reset.lock().unwrap().clone().unwrap().send(1);
                       }
                   } else if ret.term > raft.get_term() {
                       let _ret = node.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                       raft.set_state(ret.term, false, false);
                   }
               }).map_err(|_|{})
            );
        }
    }

    pub fn create_append_entries_thread(&self) {
        let (append_entries_reset, recv) = mpsc::channel();
        let node = self.clone();
        let thread1 = thread::spawn(move || loop {
            if *node.shutdown.lock().unwrap() == true {
                break;
            }
            let interval = Duration::from_millis(APPEND_ENTRIES_INTERVAL);
            if let Ok(_) = recv.recv_timeout(interval) {
                if *node.shutdown.lock().unwrap() == true {
                    break;
                }
                continue;
            } else {
                if *node.shutdown.lock().unwrap() == true {
                    break;
                }
                if node.is_leader() {
                    node.do_append_entries();
                }
            }
        });
        *self.append_entries_thread.lock().unwrap() = Some(thread1);
        *self.append_entries_reset.lock().unwrap() = Some(append_entries_reset);
    }

    pub fn do_append_entries(&self) {
        let raft = self.raft.lock().unwrap();
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
            let node = self.clone();
            let prev_log_index2 = next_index[i] - 1;
            if prev_log_index2 < raft.snapshot_index {
                // send snapshot to followers which are too late in term
                // simplified, only send once
                let args = RequestSnapshotArgs {
                    term: raft.get_term(),
                    leader_id: id as u64,
                    last_included_index: raft.snapshot_index,
                    last_included_term: raft.snapshot_term,
                    data: raft.persister.snapshot(),
                };
                peer.spawn(
                    peer.install_snapshot(&args).map(move |ret| {
                        let mut raft = node.raft.lock().unwrap();
                        if ret.term > raft.get_term() {
                            let _ret =
                                node.timeout_reset.lock().unwrap().clone().unwrap().send(1);
                            raft.set_state(ret.term, false, false);
                        }
                        if raft.is_leader() {
                            let mut next_index = raft.next_index.clone().unwrap();
                            let mut match_index = raft.match_index.clone().unwrap();
                            next_index[i] = args.last_included_index + 1;
                            match_index[i] = args.last_included_index;
                            raft.next_index = Some(next_index.clone());
                            raft.match_index = Some(match_index.clone());
                        }
                    }).map_err(|_|{})
                );
            } else {
                let mut args = RequestEntryArgs {
                    term: raft.get_term(),
                    leader_id: id as u64,
                    prev_log_index: prev_log_index2,
                    prev_log_term: 0,
                    entries: vec![],
                    leader_commit: raft.commit_index,
                };
                let entry = raft.get_log(args.prev_log_index as usize);
                args.prev_log_term = entry.unwrap().term;
                for j in 0..MAX_SEND_ONCE {
                    // send MAX_SEND_ONCE logs at most once
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
                peer.spawn(
                    peer.append_entries(&args).map(move |ret| {
                        let mut raft = node.raft.lock().unwrap();
                        if ret.success {
                            if ret.term == raft.get_term() {
                                raft.handle_success_reply(i, ret.next_index);
                            }
                        } else {
                            if ret.term > raft.get_term() {
                                let _ret = node
                                    .timeout_reset
                                    .lock()
                                    .unwrap()
                                    .clone()
                                    .unwrap()
                                    .send(1);
                                raft.set_state(ret.term, false, false);
                            } else {
                                if ret.term == raft.get_term() {
                                    raft.handle_fail_reply(
                                        i,
                                        ret.fail_type,
                                        ret.next_index,
                                        ret.conflict_term,
                                        ret.earlist_conflict_index,
                                    );
                                }
                            }
                        }
                    }).map_err(|_|{})
                );
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
        } else {
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
            is_candidate: self.is_candidate(),
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
            }
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
            Some(_) => {}
            None => {
                let last_log_index = raft.get_last_log_index() as u64;
                let last_log_term = raft.get_last_log_term();
                // election limit
                if (last_log_term == args.last_log_term && last_log_index <= args.last_log_index)
                    || last_log_term < args.last_log_term
                {
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
        my_debug!(
            "node {} receive the entry from leader {}, node.term: {}, args.term: {}",
            raft.me,
            args.leader_id,
            raft.get_term(),
            args.term
        );
        let mut reply = RequestEntryReply {
            term: raft.get_term(),
            success: false,
            next_index: raft.snapshot_index + 1,
            conflict_term: 0,
            earlist_conflict_index: raft.snapshot_index + 1,
            fail_type: 0,
        };
        if args.term < raft.get_term() {
            return Box::new(futures::future::result(Ok(reply))); // when term is wrong, return the bigger term so that the leader can revise its term
        }
        let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
        raft.set_state(args.term, false, false);
        raft.voted_for = Some(args.leader_id as usize);
        raft.persist();
        reply.term = args.term; // if term is right, return its own term
        if args.prev_log_index < raft.snapshot_index {
            return Box::new(futures::future::result(Ok(reply)));
        }
        let entry = raft.get_log(args.prev_log_index as usize);
        match entry {
            Some(en) => {
                if en.term == args.prev_log_term {
                    // check if the log is matched
                    if args.entries.len() != 0 {
                        for i in 0..args.entries.len() {
                            let log_encode = args.entries[i].clone();
                            match labcodec::decode(&log_encode) {
                                Ok(log) => {
                                    let log: Entry = log;
                                    let node_entry =
                                        raft.get_log(args.prev_log_index as usize + 1 + i);
                                    // consistency check
                                    match node_entry {
                                        Some(n_en) => {
                                            if n_en == log {
                                                // if their logs are the same, do not need to delete
                                                continue;
                                            } else {
                                                // else, delete those logs of follower after the conflict log and append leader's logs
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
                // when the log is not matched, find the eraliest log in the conflict term
                else {
                    reply.conflict_term = en.term;
                    for i in
                        (raft.snapshot_index + raft.log.len() as u64 - 1)..(raft.snapshot_index + 1)
                        {
                            let entry = raft.get_log(i as usize);
                            match entry {
                                Some(en) => {
                                    if en.term == reply.conflict_term {
                                        reply.earlist_conflict_index = i;
                                    }
                                }
                                None => {}
                            }
                        }
                    return Box::new(futures::future::result(Ok(reply)));
                }
            }
            None => {
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
            my_debug!(
                "warn:me[{}:{}] recv snapshot [{}:{}]",
                raft.me,
                raft.snapshot_index,
                args.leader_id,
                args.last_included_index
            );
            return Box::new(futures::future::result(Ok(reply)));
        }
        let _ret = self.timeout_reset.lock().unwrap().clone().unwrap().send(1);
        if args.term == raft.get_term() && raft.is_candidate() {
            raft.set_state(args.term, false, false);
            raft.voted_for = Some(args.leader_id as usize);
            raft.persist();
        }
        if args.term > raft.get_term() {
            raft.set_state(args.term, false, false);
            raft.voted_for = Some(args.leader_id as usize);
            raft.persist();
        }
        reply.term = raft.get_term();

        if args.last_included_index > (raft.snapshot_index + raft.log.len() as u64 - 1) {
            let log = Entry {
                term: args.last_included_term,
                entry: vec![],
            };
            raft.log = vec![log];
        } else {
            raft.delete_prev_log(args.last_included_index as usize);
        }
        raft.snapshot_index = args.last_included_index;
        raft.snapshot_term = args.last_included_term;
        raft.commit_index = args.last_included_index;
        raft.last_applied = args.last_included_index;

        raft.save_state_and_snapshot(args.data.clone());

        let mesg = ApplyMsg {
            command_valid: true,
            command: vec![],
            command_index: raft.snapshot_index,
            is_snapshot: true,
            data: args.data.clone(),
        };
        let _ret = raft.apply_ch.unbounded_send(mesg);

        Box::new(futures::future::result(Ok(reply)))
    }
}
