// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::{ReplyMessage, SendMessage};
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
  T: Send + Sync + 'static + Clone,
{
  protocol: P,
  pool: Arc<RwLock<HashMap<u64, Thread>>>,
  execution_queue: Arc<RwLock<HashMap<u64, VecDeque<SendMessage<T>>>>>,
}

impl<T: Send + Sync + 'static + Clone, P: Protocol<T> + Send + Sync + 'static>
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
        let mut guard = execution_queue.write();
        let mut events = guard.remove(&thread_id).unwrap_or_default();
        while !events.is_empty() {
          let event = events.pop_front().expect("Deque must not be empty");
          let reply = protocol.act(event);
          match reply {
            ReplyMessage::Oneshot { receiver_id, data } => {
              guard
                .get_mut(&receiver_id)
                .expect("Deque must not be empty")
                .push_back(SendMessage::Oneshot {
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
            ReplyMessage::None => {}
            ReplyMessage::Broadcast { data } => {
              guard.iter_mut().filter(|(id, _)| thread_id != **id).for_each(
                move |(id, deque)| {
                  deque.push_back(SendMessage::Oneshot {
                    sender_id: thread_id,
                    receiver_id: id.clone(),
                    data: data.clone(),
                  });
                },
              );

              pool.write().iter().for_each(|(_, thread)| {
                thread.unpark();
              })
            }
          }
        }

        guard.insert(thread_id, events);
        drop(guard);
        thread::park();
      }
    })?;

    self
      .pool
      .write()
      .insert(worker.thread().id().as_u64().get(), worker.thread().clone());

    Ok(())
  }

  pub fn send(&mut self, message: SendMessage<T>) {
    match message {
      SendMessage::None => {}
      SendMessage::Broadcast { sender_id, data } => {
        let thread_id = thread::current().id().as_u64().get();
        let mut guard = self.execution_queue.write();
        guard.iter_mut().filter(|(id, _)| thread_id != **id).for_each(
          move |(id, deque)| {
            deque.push_back(SendMessage::Oneshot {
              sender_id: thread_id,
              receiver_id: id.clone(),
              data: data.clone(),
            });
          },
        );

        self.pool.write().iter().for_each(|(_, thread)| {
          thread.unpark();
        })
      }
      SendMessage::Oneshot {
        sender_id,
        receiver_id,
        data,
      } => {
        let thread_id = thread::current().id().as_u64().get();
        let mut guard = self.execution_queue.write();
        guard
          .get_mut(&receiver_id)
          .expect("Deque must not be empty")
          .push_back(SendMessage::Oneshot {
            sender_id: thread_id,
            receiver_id,
            data,
          });
        self
          .pool
          .write()
          .get(&receiver_id)
          .expect("Receiver must exist.")
          .unpark();
      }
    }
  }
}
