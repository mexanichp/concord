// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::Message;
use crate::protocol::Protocol;
use std::collections::{HashMap, VecDeque};
use std::io::Error;
use std::sync::nonpoison::RwLock;
use std::sync::Arc;
use std::thread;
use std::thread::Thread;

pub struct Coordinator<T, P>
where
  P: Protocol<T> + Send + Sync + 'static,
  T: Send + Sync + 'static,
{
  protocol: P,
  pool: Arc<RwLock<HashMap<u64, Thread>>>,
  execution_queue: Arc<RwLock<HashMap<u64, VecDeque<Message<T>>>>>,
}

impl<T: Send + Sync + 'static, P: Protocol<T> + Send + Sync + 'static>
  Coordinator<T, P>
{
  pub fn new(protocol: P) -> Self {
    Self {
      protocol,
      pool: Arc::new(RwLock::new(HashMap::new())),
      execution_queue: Arc::new(RwLock::new(HashMap::new())),
    }
  }

  pub fn register(&mut self) -> Result<(), Error> {
    let protocol = self.protocol.clone();
    let execution_queue = self.execution_queue.clone();
    let pool = self.pool.clone();
    let worker = thread::Builder::new().spawn(move || {
      loop {
        let thread_id = thread::current().id().as_u64().get();
        let mut lock = execution_queue.write();
        let events = lock.entry(thread_id).or_default();

        while !events.is_empty() {
          let event = events.pop_front().expect("Deque must not be empty");
          let reply = protocol.act(event);
          match reply {
            Message::Oneshot {
              sender_id,
              receiver_id,
              data,
            } => {
              lock
                .get_mut(&receiver_id)
                .expect("Deque must not be empty")
                .push_back(Message::Oneshot {
                  sender_id: thread_id,
                  receiver_id,
                  data,
                });
              pool
                .write()
                .get(&receiver_id)
                .expect("Receiver must exist.")
                .unpark();
            }
            Message::None => {}
            Message::Broadcast { sender_id, data } => {}
          }
        }

        drop(lock);
        thread::park();
      }
    })?;

    self
      .pool
      .write()
      .insert(worker.thread().id().as_u64().get(), worker.thread().clone());

    Ok(())
  }
}
