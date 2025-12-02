// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::CoordinatorMessage;
use crate::protocol::paxos::PaxosRole::Proposer;
use crate::protocol::Protocol;
use std::thread;

pub struct Paxos {
  role: PaxosRole,
  logical_clock: u64,
  last_proposal: u64,
  acceptors_count: u64,
  value: Option<String>,
}

#[derive(Clone, Debug)]
pub enum PaxosCommand {
  Prepare { proposal: u64 },
  AckPrepare { proposal: u64 },
  Accept { proposal: u64, data: String },
  AckAccept { proposal: u64, data: String },
  Learn { proposal: u64, data: String },
}

pub enum PaxosRole {
  Proposer,
  Acceptor,
  Learner,
}

impl Protocol<PaxosCommand> for Paxos {
  fn new() -> Self {
    Self {
      role: Proposer,
      logical_clock: 0,
      last_proposal: 0,
      acceptors_count: 0,
      value: None,
    }
  }

  fn act(
    &self,
    command: CoordinatorMessage<PaxosCommand>,
  ) -> CoordinatorMessage<PaxosCommand> {
    match command {
      CoordinatorMessage::None => CoordinatorMessage::None,
      CoordinatorMessage::Broadcast { sender_id, data } => {
        Self::log(&sender_id, &data);

        CoordinatorMessage::None
      }
      CoordinatorMessage::Oneshot {
        sender_id,
        receiver_id: _,
        data,
      } => {
        Self::log(&sender_id, &data);

        CoordinatorMessage::None
      }
    }
  }
}

impl Paxos {
  fn log(sender_id: &u64, data: &PaxosCommand) {
    println!(
      "Sender {sender_id:?} sent {data:?} to {:?}",
      thread::current().id()
    );
  }
}
