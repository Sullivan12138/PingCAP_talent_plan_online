use crate::protos;
use grpcio::{ChannelBuilder, EnvBuilder};
use std::sync::Arc;

use protos::kvserver::{DeleteRequest, GetRequest, PutRequest, ResponseStatus, ScanRequest};
use protos::kvserver_grpc::KvdbClient;

use std::collections::HashMap;
pub struct Client {
    pub client: KvdbClient,
}

impl Client {
    pub fn new(host: String, port: u16) -> Self {
        let addr = format!("{}:{}", host, port);
        let env = Arc::new(EnvBuilder::new().build());
        let ch = ChannelBuilder::new(env).connect(addr.as_ref());
        let kv_client = KvdbClient::new(ch);

        Client { client: kv_client }
    }
    pub fn get(&self, key: String) -> Option<String> {
        let mut request = GetRequest::new();
        request.set_key(key);
        let ret = self.client.get(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => Some(ret.value),
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => None,
        }
    }
    pub fn put(&self, key: String, value: String) -> bool {
        let mut request = PutRequest::new();

        request.set_key(key);
        request.set_value(value);
        let ret = self.client.put(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => true,
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => false,
        }
    }
    pub fn delete(&self, key: String) -> bool {
        let mut request = DeleteRequest::new();
        request.set_key(key);
        let ret = self.client.delete(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess | ResponseStatus::kNotFound => true,
            ResponseStatus::kFailed | ResponseStatus::kNoType => false,
        }
    }
    pub fn scan(&self, key_start: String, key_end: String) -> Option<HashMap<String, String>> {
        let mut request = ScanRequest::new();
        request.set_key_start(key_start);
        request.set_key_end(key_end);
        let ret = self.client.scan(&request).expect("RPC failed");
        match ret.status {
            ResponseStatus::kSuccess => Some(ret.key_value),
            ResponseStatus::kNotFound | ResponseStatus::kFailed | ResponseStatus::kNoType => None,
        }
    }
}
