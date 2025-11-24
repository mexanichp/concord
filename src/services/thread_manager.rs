// SPDX-License-Identifier: GPL-3.0+

use crate::models::thread_command::ThreadCommand;
use std::collections::{HashMap, HashSet, VecDeque};
use std::process::exit;
use std::sync::Arc;
use std::sync::nonpoison::RwLock;
use std::thread;
use std::thread::{Thread, ThreadId};

pub struct ThreadManager {
  threads: HashMap<u64, Thread>,
  data_store: Arc<RwLock<HashMap<u64, VecDeque<Arc<ThreadCommand>>>>>,
}

impl ThreadManager {
  pub fn new() -> Self {
    Self {
      threads: HashMap::new(),
      data_store: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  pub fn spawn(&mut self) {
    let data_store = self.data_store.clone();
    let handle = thread::spawn(move || {
      loop {
        let mut write_lock = data_store.write();
        let events = write_lock
          .entry(thread::current().id().as_u64().get())
          .or_insert(VecDeque::new());
        while !events.is_empty() {
          let event = events.pop_front().unwrap();
          match *event {
            ThreadCommand::EXIT => {
              exit(0);
            }
            ThreadCommand::DATA(sender, ref data) => {
              println!(
                "Thread #{:?} received data: {} from: {:?}",
                thread::current().id(),
                data,
                sender
              );
            }
            ThreadCommand::KILL(_) => {
              panic!("Killing thread called.")
            }
            _ => {
              panic!("Unknown command.")
            }
          }
        }
        drop(write_lock);
        thread::park();
      }
    });
    let thread = handle.thread().clone();
    self.threads.insert(thread.id().as_u64().get(), thread.clone());
    println!("Spawned thread with id: {:?}", thread.id())
  }

  pub fn send(&mut self, event: ThreadCommand) {
    println!(
      "Sending command {event:?} from thread {:?}",
      thread::current().id()
    );
    match event {
      ThreadCommand::BROADCAST(command) => {
        let mut guard = self.data_store.write();
        guard.iter_mut().for_each(|(&thread, values)| {
          values.push_back(command.clone());
          self.threads.get(&thread).unwrap().unpark();
        });
      }
      ThreadCommand::KILL(thread_id) => {
        let mut guard = self.data_store.write();
        guard
          .get_mut(&thread_id)
          .unwrap()
          .push_back(Arc::new(ThreadCommand::KILL(thread_id)));
        self.threads.get(&thread_id).unwrap().unpark();
      }
      ThreadCommand::SEND(receiver, command) => {
        let mut guard = self.data_store.write();
        match guard.get_mut(&receiver) {
          None => {}
          Some(values) => {
            values.push_back(command.clone());
          }
        };
      }
      _ => {
        println!("Unsupported send command {event:?}")
      }
    }
  }
}
