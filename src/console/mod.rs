// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::coordinator::Coordinator;
use crate::models::thread_command::ThreadCommand;
use crate::protocol::paxos::Paxos;
use crate::services::thread_manager::ThreadManager;
use std::io::Error;
use std::process::exit;
use std::sync::nonpoison::Mutex;
use std::sync::Arc;
use std::{io, thread};

pub fn run() -> Result<(), Error> {
  let thread_manager = Arc::new(Mutex::new(ThreadManager::new()));
  let mut coordinator = Coordinator::new(Paxos);

  loop {
    let thread_manager = thread_manager.clone();
    let mut input = String::new();
    match io::stdin().read_line(&mut input) {
      Ok(_) => match input.trim() {
        s if s.starts_with("kill") => {
          let s: Vec<&str> = s.split(" ").collect();
          thread_manager.lock().send(ThreadCommand::KILL(
            u64::from_ascii(s[1].as_bytes()).unwrap(),
          ))
        }
        "help" => {
          println!("use spawn for new thread")
        }
        "spawn" => {
          // thread_manager.lock().spawn();
          coordinator.register()?;
        }
        "broadcast" => thread_manager.lock().send(ThreadCommand::BROADCAST(
          Arc::new(ThreadCommand::DATA(
            thread::current().id().as_u64().get(),
            "Hey From".into(),
          )),
        )),
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
