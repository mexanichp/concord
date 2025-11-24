// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::{ReplyMessage, SendMessage};

pub mod paxos;

pub trait Protocol<T: Clone>: Clone {
  fn act(&self, command: SendMessage<T>) -> ReplyMessage<T>;
}
