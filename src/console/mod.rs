// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::coordinator::Coordinator;
use crate::coordination::messages::CoordinatorMessage;
use crate::protocol::paxos::{Paxos, PaxosCommand};
use std::io;
use std::io::Error;
use std::process::exit;
use std::sync::nonpoison::Mutex;
use std::sync::Arc;

pub fn run() -> Result<(), Error> {
  let coordinator = Arc::new(Mutex::new(Coordinator::<_, Paxos>::new()));

  loop {
    let coordinator = coordinator.clone();
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
      Ok(_) => match input.trim() {
        "help" => {
          // TODO
        }
        "spawn" => {
          coordinator.lock().register()?;
        }
        "broadcast" => coordinator.lock().send(CoordinatorMessage::Broadcast {
          sender_id: 0,
          data: PaxosCommand::Prepare { proposal: 2 },
        }),
        "exit" => {
          exit(0);
        }
        _ => {
          println!("Unknown command {input:?}")
        }
      },
      Err(_) => {
        exit(1);
      }
    };
  }
}
