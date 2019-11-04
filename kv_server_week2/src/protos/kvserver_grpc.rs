// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_KVDB_GET: ::grpcio::Method<super::kvserver::GetRequest, super::kvserver::GetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kvserver.Kvdb/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KVDB_PUT: ::grpcio::Method<super::kvserver::PutRequest, super::kvserver::PutResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kvserver.Kvdb/Put",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KVDB_DELETE: ::grpcio::Method<super::kvserver::DeleteRequest, super::kvserver::DeleteResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kvserver.Kvdb/Delete",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KVDB_SCAN: ::grpcio::Method<super::kvserver::ScanRequest, super::kvserver::ScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kvserver.Kvdb/Scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct KvdbClient {
    client: ::grpcio::Client,
}

impl KvdbClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        KvdbClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_opt(&self, req: &super::kvserver::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvserver::GetResponse> {
        self.client.unary_call(&METHOD_KVDB_GET, req, opt)
    }

    pub fn get(&self, req: &super::kvserver::GetRequest) -> ::grpcio::Result<super::kvserver::GetResponse> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: &super::kvserver::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::GetResponse>> {
        self.client.unary_call_async(&METHOD_KVDB_GET, req, opt)
    }

    pub fn get_async(&self, req: &super::kvserver::GetRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::GetResponse>> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_opt(&self, req: &super::kvserver::PutRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvserver::PutResponse> {
        self.client.unary_call(&METHOD_KVDB_PUT, req, opt)
    }

    pub fn put(&self, req: &super::kvserver::PutRequest) -> ::grpcio::Result<super::kvserver::PutResponse> {
        self.put_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_async_opt(&self, req: &super::kvserver::PutRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::PutResponse>> {
        self.client.unary_call_async(&METHOD_KVDB_PUT, req, opt)
    }

    pub fn put_async(&self, req: &super::kvserver::PutRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::PutResponse>> {
        self.put_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_opt(&self, req: &super::kvserver::DeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvserver::DeleteResponse> {
        self.client.unary_call(&METHOD_KVDB_DELETE, req, opt)
    }

    pub fn delete(&self, req: &super::kvserver::DeleteRequest) -> ::grpcio::Result<super::kvserver::DeleteResponse> {
        self.delete_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_async_opt(&self, req: &super::kvserver::DeleteRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::DeleteResponse>> {
        self.client.unary_call_async(&METHOD_KVDB_DELETE, req, opt)
    }

    pub fn delete_async(&self, req: &super::kvserver::DeleteRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::DeleteResponse>> {
        self.delete_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::kvserver::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kvserver::ScanResponse> {
        self.client.unary_call(&METHOD_KVDB_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::kvserver::ScanRequest) -> ::grpcio::Result<super::kvserver::ScanResponse> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_async_opt(&self, req: &super::kvserver::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::ScanResponse>> {
        self.client.unary_call_async(&METHOD_KVDB_SCAN, req, opt)
    }

    pub fn scan_async(&self, req: &super::kvserver::ScanRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kvserver::ScanResponse>> {
        self.scan_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait Kvdb {
    fn get(&mut self, ctx: ::grpcio::RpcContext, req: super::kvserver::GetRequest, sink: ::grpcio::UnarySink<super::kvserver::GetResponse>);
    fn put(&mut self, ctx: ::grpcio::RpcContext, req: super::kvserver::PutRequest, sink: ::grpcio::UnarySink<super::kvserver::PutResponse>);
    fn delete(&mut self, ctx: ::grpcio::RpcContext, req: super::kvserver::DeleteRequest, sink: ::grpcio::UnarySink<super::kvserver::DeleteResponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::kvserver::ScanRequest, sink: ::grpcio::UnarySink<super::kvserver::ScanResponse>);
}

pub fn create_kvdb<S: Kvdb + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KVDB_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KVDB_PUT, move |ctx, req, resp| {
        instance.put(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KVDB_DELETE, move |ctx, req, resp| {
        instance.delete(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KVDB_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
