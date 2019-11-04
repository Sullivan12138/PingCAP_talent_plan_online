#[cfg(test)]
mod tests {
use kv_server_week1::protos;
use kv_server_week1::server;
use kv_server_week1::client;
use std::sync::Arc;
use grpcio::{Environment, ServerBuilder};
use protos::kvserver_grpc;
use server::server::DbService;
use client::client::Client;
use std::collections::HashMap;
    #[test]
    fn test_set_and_get() {
        let env = Arc::new(Environment::new(1));
        let service  = kvserver_grpc::create_kvdb(DbService::new());
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind("127.0.0.1", 20001)
            .build()
            .unwrap();

        server.start();
        for &(ref host, port) in server.bind_addrs() {
            println!("listening on {}:{}", host, port);
        }
        let test_host = String::from("127.0.0.1");
        let test_port = 20001;

        let client = Client::new(test_host.clone(), test_port);
        client.put("aa".to_string(),"aaaaa".to_string());
        client.put("bb".to_string(),"bbbbb".to_string());
        client.put("cc".to_string(),"ccccc".to_string());
        let ret = client.get("aa".to_string());
        match ret {
            Some(v) => {
                assert_eq!(v, "aaaaa");
            },
            None => {
                panic!("Not get value of \"aa\"");
            }
        }
        client.put("aa".to_string(), "abcde".to_string());
        let ret = client.get("aa".to_string());
        match ret {
            Some(v) => {
                assert_eq!(v, "abcde");
            },
            None => {
                panic!("Not get value of \"aa\"");
            }
        }
        client.delete("aa".to_string());
        client.put("dd".to_string(),"ccccc".to_string());
        client.put("dd".to_string(),"ddddd".to_string());
        let ret = client.scan("aa".to_string(),"ee".to_string());
        match ret {
            Some(v) => println!("scan{{ {:?} }}",v),
            None => println!("scan None")
        }
    }
    #[test]
    fn test_delete() {
        let env = Arc::new(Environment::new(1));
        let service  = kvserver_grpc::create_kvdb(DbService::new());
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind("127.0.0.1", 20001)
            .build()
            .unwrap();

        server.start();
        for &(ref host, port) in server.bind_addrs() {
            println!("listening on {}:{}", host, port);
        }
        let test_host = String::from("127.0.0.1");
        let test_port = 20001;

        let client = Client::new(test_host.clone(), test_port);
        client.put("aa".to_string(),"aaaaa".to_string());
        client.put("bb".to_string(),"bbbbb".to_string());
        client.put("cc".to_string(),"ccccc".to_string());
        client.delete("aa".to_string());
        let ret = client.get("aa".to_string());
        assert_eq!(ret, None);
        client.put("dd".to_string(),"ccccc".to_string());
        client.put("dd".to_string(),"ddddd".to_string());
        let ret = client.scan("aa".to_string(),"ee".to_string());
        match ret {
            Some(v) => println!("scan{{ {:?} }}",v),
            None => println!("scan None")
        }
    }
    #[test]
    fn test_scan() {
        let env = Arc::new(Environment::new(1));
        let service  = kvserver_grpc::create_kvdb(DbService::new());
        let mut server = ServerBuilder::new(env)
            .register_service(service)
            .bind("127.0.0.1", 20001)
            .build()
            .unwrap();

        server.start();
        for &(ref host, port) in server.bind_addrs() {
            println!("listening on {}:{}", host, port);
        }
        let test_host = String::from("127.0.0.1");
        let test_port = 20001;

        let client = Client::new(test_host.clone(), test_port);
        client.put("aa".to_string(),"aaaaa".to_string());
        client.put("bb".to_string(),"bbbbb".to_string());
        client.put("cc".to_string(),"ccccc".to_string());
        client.put("dd".to_string(),"ddddd".to_string());
        client.put("ee".to_string(),"eeeee".to_string());
        let ret = client.scan("aa".to_string(),"ee".to_string());
        let mut hashmap = HashMap::new();
        hashmap.insert("bb".to_string(), "bbbbb".to_string());
        hashmap.insert("cc".to_string(), "ccccc".to_string());
        hashmap.insert("dd".to_string(), "ddddd".to_string());
        match ret {
            Some(v) => {
                assert_eq!(v, hashmap);
            },
            None => {
                panic!("cannot scan anything");
            }
        }
    }
}
pub fn main() {}