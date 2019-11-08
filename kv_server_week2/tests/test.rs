extern crate grpcio;
extern crate lib;
use chrono::prelude::*;
use grpcio::Environment;
use grpcio::ServerBuilder;
use lib::kv_client::Client;
use lib::kv_server::DbService;
use lib::protos::kvserver_grpc;
use std::sync::Arc;
use std::thread;
// the amount of threads
const THREAD_NUM: u16 = 1000;
// the amount of set operations in one thread
const SET_NUM: u16 = 9;
// the value width set for test, it should be big
const VALUE_WIDTH: usize = 128;
const TIME_THRESHOLD: f64 = 9.0;
// generate a value of certain width
fn gen_value(v: u64) -> String {
    format!("{:>0width$}", v, width = VALUE_WIDTH)
}
#[test]
fn test_persistence_and_concurrency() {
    let env = Arc::new(Environment::new(1));
    let db = DbService::new();

    let service = kvserver_grpc::create_kvdb(db.clone());
    let mut server = ServerBuilder::new(env.clone())
        .register_service(service)
        .bind("127.0.0.1", 20001)
        .build()
        .unwrap();

    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let start_time = Local::now();
    let mut handles = vec![];
    let test_host = "127.0.0.1";
    let test_port = 20001;
    // test the concurrency
    // the required concurrency should > 1000
    for _i in 0..THREAD_NUM {
        let handle = thread::spawn(move || {
            let client = Client::new(test_host.to_owned(), test_port);
            for _j in 0..SET_NUM {
                client.put("aa".to_owned(), gen_value(1234567));
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
    let end_time = Local::now();
    let use_time = (end_time.timestamp_millis() - start_time.timestamp_millis()) as f64 / 1000.0;
    assert!(use_time < TIME_THRESHOLD);
    let client = Client::new(test_host.to_owned(), test_port);
    // set some special characters to test
    client.put("b\0b".to_owned(), "22\02".to_owned());
    client.put("c\tc".to_owned(), "33\t3".to_owned());
    client.put("d\nd".to_owned(), "44\n4".to_owned());
    client.put("ee".to_owned(), "555".to_owned());
    // test the delete operation
    client.delete("ee".to_owned());
    server.shutdown();
    // then restart the server
    let service = kvserver_grpc::create_kvdb(db.clone());
    let mut server = ServerBuilder::new(env.clone())
        .register_service(service)
        .bind("127.0.0.1", 20001)
        .build()
        .unwrap();
    server.start();
    assert_eq!(client.get("b\0b".to_owned()), Some("22\02".to_owned()));
    assert_eq!(client.get("c\tc".to_owned()), Some("33\t3".to_owned()));
    assert_eq!(client.get("d\nd".to_owned()), Some("44\n4".to_owned()));
    assert_eq!(client.get("ee".to_owned()), None);
}
