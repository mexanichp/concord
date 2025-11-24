// SPDX-License-Identifier: GPL-3.0+

#![feature(nonpoison_rwlock)]
#![feature(sync_nonpoison)]
#![feature(thread_id_value)]
#![feature(int_from_ascii)]
#![feature(nonpoison_mutex)]

mod console;
mod coordination;
mod models;
mod protocol;
mod services;

use std::io::Error;

fn main() -> Result<(), Error> {
  console::run()
}
