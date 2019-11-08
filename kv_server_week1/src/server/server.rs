extern crate futures;
extern crate grpcio;
extern crate protobuf;

use crate::engine;
use crate::protos;

use std::io::Read;
use std::sync::Arc;
use std::{io, thread};

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use protos::kvserver::{
    DeleteRequest, DeleteResponse, GetRequest, GetResponse, PutRequest, PutResponse,
    ResponseStatus, ScanRequest, ScanResponse,
};
use protos::kvserver_grpc::{self, Kvdb};

use engine::dbengine::DbEngine;

#[derive(Clone)]
pub struct DbService {
    db_engine: DbEngine,
}

impl Kvdb for DbService {
    fn get(&mut self, ctx: RpcContext, req: GetRequest, sink: UnarySink<GetResponse>) {
        let mut response = GetResponse::new();
        println!("Received GetRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.get(&req.key);
        match ret {
            Ok(op) => match op {
                Some(value) => {
                    response.set_status(ResponseStatus::kSuccess);
                    response.set_value(value);
                }
                None => response.set_status(ResponseStatus::kNotFound),
            },
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }

        let f = sink
            .success(response.clone())
            .map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn put(&mut self, ctx: RpcContext, req: PutRequest, sink: UnarySink<PutResponse>) {
        let mut response = PutResponse::new();
        println!("Received PutRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.put(&req.key, &req.value);
        match ret {
            Ok(_) => {
                response.set_status(ResponseStatus::kSuccess);
            }
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }
        let f = sink
            .success(response.clone())
            .map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn delete(&mut self, ctx: RpcContext, req: DeleteRequest, sink: UnarySink<DeleteResponse>) {
        let mut response = DeleteResponse::new();
        println!("Received DeleteResponse {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.delete(&req.key);
        match ret {
            Ok(op) => match op {
                Some(_) => response.set_status(ResponseStatus::kSuccess),
                None => response.set_status(ResponseStatus::kNotFound),
            },
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }
        let f = sink
            .success(response.clone())
            .map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
    fn scan(&mut self, ctx: RpcContext, req: ScanRequest, sink: UnarySink<ScanResponse>) {
        let mut response = ScanResponse::new();
        println!("Received ScanRequest {{ {:?} }}", req);
        let engine = &mut self.db_engine;
        let ret = engine.scan(&req.key_start, &req.key_end);
        match ret {
            Ok(op) => match op {
                Some(key_value) => {
                    response.set_status(ResponseStatus::kSuccess);
                    response.set_key_value(key_value);
                }
                None => response.set_status(ResponseStatus::kNotFound),
            },
            Err(_) => response.set_status(ResponseStatus::kFailed),
        }

        let f = sink
            .success(response.clone())
            .map(move |_| println!("Responded with  {{ {:?} }}", response))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
}

impl DbService {
    pub fn new() -> Self {
        println!("new DbService");
        DbService {
            db_engine: DbEngine::new(),
        }
    }
}
