// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::coordinator::Coordinator;
use crate::coordination::messages::SendMessage;
use crate::protocol::paxos::{Paxos, PaxosCommand};
use std::io;
use std::io::Error;
use std::process::exit;
use std::sync::nonpoison::Mutex;
use std::sync::Arc;

pub fn run() -> Result<(), Error> {
  let mut coordinator = Arc::new(Mutex::new(Coordinator::new(Paxos)));

  loop {
    let coordinator = coordinator.clone();
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
      Ok(_) => match input.trim() {
        // s if s.starts_with("kill") => {
        //   let s: Vec<&str> = s.split(" ").collect();
        //   coordinator.lock().send(ThreadCommand::KILL(
        //     u64::from_ascii(s[1].as_bytes()).unwrap(),
        //   ))
        // }
        "help" => {
          println!("use spawn for new thread")
        }
        "spawn" => {
          // thread_manager.lock().spawn();
          coordinator.lock().register()?;
        }
        "broadcast" => coordinator.lock().send(SendMessage::Broadcast {
          sender_id: 0,
          data: PaxosCommand::Data { s: "Hello!".into() },
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
