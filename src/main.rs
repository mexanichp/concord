// SPDX-License-Identifier: GPL-3.0+

#![feature(nonpoison_rwlock)]
#![feature(sync_nonpoison)]
#![feature(thread_id_value)]
#![feature(int_from_ascii)]
#![feature(nonpoison_mutex)]
#![feature(fn_traits)]

mod console;
mod coordination;
mod protocol;

use std::io::Error;

fn main() -> Result<(), Error> {
  console::run()
}
