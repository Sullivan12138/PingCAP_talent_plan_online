// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy::all)]

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

const METHOD_KV_DB_GET: ::grpcio::Method<super::kv_server::GetRequest, super::kv_server::GetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kv_server.KvDb/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_DB_SET: ::grpcio::Method<super::kv_server::SetRequest, super::kv_server::SetResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kv_server.KvDb/Set",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_DB_DELETE: ::grpcio::Method<super::kv_server::DelRequest, super::kv_server::DelResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kv_server.KvDb/Delete",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_DB_SCAN: ::grpcio::Method<super::kv_server::ScanRequest, super::kv_server::ScanResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/kv_server.KvDb/Scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct KvDbClient {
    client: ::grpcio::Client,
}

impl KvDbClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        KvDbClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn get_opt(&self, req: &super::kv_server::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kv_server::GetResponse> {
        self.client.unary_call(&METHOD_KV_DB_GET, req, opt)
    }

    pub fn get(&self, req: &super::kv_server::GetRequest) -> ::grpcio::Result<super::kv_server::GetResponse> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: &super::kv_server::GetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::GetResponse>> {
        self.client.unary_call_async(&METHOD_KV_DB_GET, req, opt)
    }

    pub fn get_async(&self, req: &super::kv_server::GetRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::GetResponse>> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn set_opt(&self, req: &super::kv_server::SetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kv_server::SetResponse> {
        self.client.unary_call(&METHOD_KV_DB_SET, req, opt)
    }

    pub fn set(&self, req: &super::kv_server::SetRequest) -> ::grpcio::Result<super::kv_server::SetResponse> {
        self.set_opt(req, ::grpcio::CallOption::default())
    }

    pub fn set_async_opt(&self, req: &super::kv_server::SetRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::SetResponse>> {
        self.client.unary_call_async(&METHOD_KV_DB_SET, req, opt)
    }

    pub fn set_async(&self, req: &super::kv_server::SetRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::SetResponse>> {
        self.set_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_opt(&self, req: &super::kv_server::DelRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kv_server::DelResponse> {
        self.client.unary_call(&METHOD_KV_DB_DELETE, req, opt)
    }

    pub fn delete(&self, req: &super::kv_server::DelRequest) -> ::grpcio::Result<super::kv_server::DelResponse> {
        self.delete_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_async_opt(&self, req: &super::kv_server::DelRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::DelResponse>> {
        self.client.unary_call_async(&METHOD_KV_DB_DELETE, req, opt)
    }

    pub fn delete_async(&self, req: &super::kv_server::DelRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::DelResponse>> {
        self.delete_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::kv_server::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::kv_server::ScanResponse> {
        self.client.unary_call(&METHOD_KV_DB_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::kv_server::ScanRequest) -> ::grpcio::Result<super::kv_server::ScanResponse> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_async_opt(&self, req: &super::kv_server::ScanRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::ScanResponse>> {
        self.client.unary_call_async(&METHOD_KV_DB_SCAN, req, opt)
    }

    pub fn scan_async(&self, req: &super::kv_server::ScanRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::kv_server::ScanResponse>> {
        self.scan_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait KvDb {
    fn get(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::GetRequest, sink: ::grpcio::UnarySink<super::kv_server::GetResponse>);
    fn set(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::SetRequest, sink: ::grpcio::UnarySink<super::kv_server::SetResponse>);
    fn delete(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::DelRequest, sink: ::grpcio::UnarySink<super::kv_server::DelResponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::kv_server::ScanRequest, sink: ::grpcio::UnarySink<super::kv_server::ScanResponse>);
}

pub fn create_kv_db<S: KvDb + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_DB_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_DB_SET, move |ctx, req, resp| {
        instance.set(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_DB_DELETE, move |ctx, req, resp| {
        instance.delete(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_DB_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
