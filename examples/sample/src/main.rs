#![feature(nll)]
extern crate actix;
extern crate actix_web;
extern crate bitcask_rs;
extern crate failure;
extern crate futures;
#[macro_use]
extern crate serde_derive;

use actix::prelude::{Actor, Addr, Handler, Message, Syn, SyncArbiter, SyncContext};
use actix_web::{App, AsyncResponder, FromRequest, FutureResponse, HttpRequest, HttpResponse, Query, server};
use failure::Error;
use futures::future::{err, Future};


#[derive(Deserialize)]
struct Info {
    key: String,
    value: Option<String>,
}


fn index(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let state = req.state();
    let addr: &Addr<Syn, BitcaskActor> = &state.addr;
    addr.send(List).from_err()
        .map(move |ret|
            match ret {
                Ok(v) => HttpResponse::Ok().body(format!("{:?}", v)),
                Err(e) => HttpResponse::BadRequest().body(format!("{}", e))
            }).responder()
}

fn get(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let key = match Query::<Info>::extract(&req) {
        Ok(v) => v,
        Err(e) => return Box::new(err(e))
    };
    let state = req.state();
    let addr: &Addr<Syn, BitcaskActor> = &state.addr;
    addr.send(Get(key.key.clone())).from_err()
        .map(move |ret|
            match ret {
                Ok(v) => HttpResponse::Ok().body(format!("{}: {:?}", &key.key, v)),
                Err(e) => HttpResponse::BadRequest().body(format!("{}", e))
            }).responder()
}

fn set(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let info = match Query::<Info>::extract(&req) {
        Ok(v) => v,
        Err(e) => return Box::new(err(e))
    };
    let state = req.state();
    let addr: &Addr<Syn, BitcaskActor> = &state.addr;
    addr.send(Set(info.key.clone(), info.value.as_ref().map_or(vec![0], |v| v.as_bytes().to_vec()))).from_err()
        .map(|ret|
            match ret {
                Ok(_) => HttpResponse::Ok().body("ok"),
                Err(e) => HttpResponse::BadRequest().body(format!("{}", e))
            }).responder()
}

fn delete(req: HttpRequest<AppState>) -> FutureResponse<HttpResponse> {
    let info = match Query::<Info>::extract(&req) {
        Ok(v) => v,
        Err(e) => return Box::new(err(e))
    };
    let state = req.state();
    let addr: &Addr<Syn, BitcaskActor> = &state.addr;
    addr.send(Delete(info.key.clone())).from_err()
        .map(|ret|
            match ret {
                Ok(_) => HttpResponse::Ok().body("ok"),
                Err(e) => HttpResponse::BadRequest().body(format!("{}", e))
            }).responder()
}

struct List;

struct Get(String);

struct Set(String, Vec<u8>);

struct Delete(String);

impl Message for List {
    type Result = Result<Option<Vec<String>>, Error>;
}

impl Message for Get {
    type Result = Result<Option<Vec<u8>>, Error>;
}

impl Message for Set {
    type Result = Result<(), Error>;
}

impl Message for Delete {
    type Result = Result<(), Error>;
}


struct BitcaskActor(bitcask_rs::Bitcask);

impl Actor for BitcaskActor {
    type Context = SyncContext<Self>;
}

impl Handler<Get> for BitcaskActor {
    type Result = Result<Option<Vec<u8>>, Error>;

    fn handle(&mut self, msg: Get, _: &mut Self::Context) -> Self::Result {
        self.0.get(msg.0)
    }
}


impl Handler<Set> for BitcaskActor {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: Set, _: &mut Self::Context) -> Self::Result {
        self.0.set(msg.0, msg.1)
    }
}

impl Handler<Delete> for BitcaskActor {
    type Result = Result<(), Error>;

    fn handle(&mut self, msg: Delete, _: &mut Self::Context) -> Self::Result {
        self.0.delete(msg.0)
    }
}

impl Handler<List> for BitcaskActor {
    type Result = Result<Option<Vec<String>>, Error>;

    fn handle(&mut self, msg: List, ctx: &mut Self::Context) -> <Self as Handler<List>>::Result {
        Ok(Some(self.0.keys().into_iter().map(|k| k.clone()).collect::<Vec<String>>()))
    }
}

struct AppState {
    addr: Addr<Syn, BitcaskActor>
}


fn main() {
    bitcask_rs::setup();
    let config = bitcask_rs::Config::new("config.yml");
    let bitcask = bitcask_rs::Bitcask::open(config);
    let sys = actix::System::new("hello-world");

    let addr = SyncArbiter::start(2, move || BitcaskActor(bitcask.clone()));

    server::new(move ||
        App::with_state(AppState { addr: addr.clone() })
            .resource("/", |r| r.f(index))
            .resource("/get", |r| r.route().a(get))
            .resource("/set", |r| r.route().a(set))
            .resource("/delete", |r| r.route().a(delete))
    ).bind("127.0.0.1:8088")
        .unwrap()
        .start();
    sys.run();
}
