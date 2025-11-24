// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::{ReplyMessage, SendMessage};
use crate::protocol::Protocol;
use std::thread;

#[derive(Clone)]
pub struct Paxos;

#[derive(Clone, Debug)]
pub enum PaxosCommand {
  Data { s: String },
}

pub enum PaxosReply {
  Continue { receiver: u64, data: String },
  Broadcast { data: String },
  Finish,
}

impl Protocol<PaxosCommand> for Paxos {
  fn act(
    &self,
    command: SendMessage<PaxosCommand>,
  ) -> ReplyMessage<PaxosCommand> {
    match command {
      SendMessage::None => ReplyMessage::None,
      SendMessage::Broadcast { sender_id, data } => {
        println!("Sender {sender_id:?} sends {data:?}");
        ReplyMessage::None
      }
      SendMessage::Oneshot {
        sender_id,
        receiver_id,
        data,
      } => {
        println!(
          "Sender {sender_id:?} sends {data:?} to {:?}",
          thread::current().id()
        );
        ReplyMessage::None
      }
    }
  }
}
