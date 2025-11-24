// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::Message;

pub mod paxos;

pub trait Protocol<T>: Clone {
  fn act(&self, command: Message<T>) -> Message<T>;
}
