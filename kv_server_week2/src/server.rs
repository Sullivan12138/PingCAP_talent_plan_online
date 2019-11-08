use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, ServerBuilder};
use lib::kv_server::DbService;
use lib::protos::kvserver_grpc;
use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

fn main() {
    let env = Arc::new(Environment::new(1));
    let db = DbService::new();
    let service = kvserver_grpc::create_kvdb(db.clone());
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 20001)
        .build()
        .unwrap();

    server.start();
    for &(ref host, port) in server.bind_addrs() {
        println!("listening on {}:{}", host, port);
    }
    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });

    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
