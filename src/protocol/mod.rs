// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::CoordinatorMessage;

pub mod paxos;

pub trait Protocol<T: Clone> {
  fn new() -> Self;
  fn act(&self, command: CoordinatorMessage<T>) -> CoordinatorMessage<T>;
}
