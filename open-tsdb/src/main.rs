#![allow(dead_code)]
mod db;
mod delta;
mod head;
mod index;
mod model;
mod otel;
mod promql;
mod serde;
mod storage;
mod util;

fn main() {
    println!("open-tsdb: timeseries store");
}
