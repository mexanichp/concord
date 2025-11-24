// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::ReplyMessage;
use crate::protocol::Protocol;

#[derive(Clone)]
pub struct Paxos;

pub enum PaxosCommand {
  Send { receiver: u64, data: String },
  Broadcast { data: String },
  HandleReply { data: String },
}

pub enum PaxosReply {
  Continue { receiver: u64, data: String },
  Broadcast { data: String },
  Finish,
}

impl Protocol<PaxosCommand, PaxosReply> for Paxos {
  fn act(&self, command: PaxosCommand) -> ReplyMessage<PaxosReply> {
    todo!()
  }
}
