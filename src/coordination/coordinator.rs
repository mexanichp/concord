// SPDX-License-Identifier: GPL-3.0+

use crate::coordination::messages::CoordinatorMessage;
use crate::protocol::Protocol;
use std::collections::{HashMap, VecDeque};
use std::io::Error;
use std::marker::PhantomData;
use std::sync::nonpoison::RwLock;
use std::sync::Arc;
use std::thread;
use std::thread::Thread;

pub struct Coordinator<T, P>
where
  P: Protocol<T> + Send + Sync + 'static,
  T: Send + Sync + 'static + Clone,
{
  pool: Arc<RwLock<HashMap<u64, Thread>>>,
  execution_queue: Arc<RwLock<HashMap<u64, VecDeque<CoordinatorMessage<T>>>>>,
  _p: PhantomData<P>,
}

impl<T: Send + Sync + 'static + Clone, P: Protocol<T> + Send + Sync + 'static>
  Coordinator<T, P>
{
  pub fn new() -> Self {
    Self {
      pool: Arc::new(RwLock::new(HashMap::new())),
      execution_queue: Arc::new(RwLock::new(HashMap::new())),
      _p: Default::default(),
    }
  }

  pub fn register(&mut self) -> Result<(), Error> {
    let protocol = P::new();
    let execution_queue_mutex = self.execution_queue.clone();
    let pool = self.pool.clone();
    let worker = thread::Builder::new().spawn(move || {
      loop {
        let thread_id = thread::current().id().as_u64().get();
        let mut execution_lock = execution_queue_mutex.write();
        let mut events = execution_lock.remove(&thread_id).unwrap_or_default();
        while !events.is_empty() {
          let event = events.pop_front().expect("Deque must not be empty");
          let reply = protocol.act(event);
          let pool = pool.read();
          Self::process(reply, &mut execution_lock, &pool);
        }
        execution_lock.insert(thread_id, events);
        drop(execution_lock);
        thread::park();
      }
    })?;

    self
      .pool
      .write()
      .insert(worker.thread().id().as_u64().get(), worker.thread().clone());

    Ok(())
  }

  pub fn send(&mut self, message: CoordinatorMessage<T>) {
    Self::process(
      message,
      &mut self.execution_queue.write(),
      &self.pool.read(),
    );
  }

  fn process(
    message: CoordinatorMessage<T>,
    execution_queue: &mut HashMap<u64, VecDeque<CoordinatorMessage<T>>>,
    pool: &HashMap<u64, Thread>,
  ) {
    let thread_id = thread::current().id().as_u64().get();

    match &message {
      CoordinatorMessage::None => {}
      CoordinatorMessage::Oneshot {
        sender_id: _,
        receiver_id,
        data: _,
      } => {
        execution_queue
          .get_mut(receiver_id)
          .expect("Deque must not be empty")
          .push_back(message.clone());

        pool.get(receiver_id).expect("Receiver must exist.").unpark();
      }
      CoordinatorMessage::Broadcast { sender_id: _, data } => {
        Self::broadcast(data.clone(), thread_id, execution_queue);
        Self::notify_all(pool);
      }
    }
  }

  fn notify_all(pool: &HashMap<u64, Thread>) {
    pool.iter().for_each(|(_, thread)| {
      thread.unpark();
    })
  }

  fn broadcast(
    data: T,
    current_thread_id: u64,
    guard: &mut HashMap<u64, VecDeque<CoordinatorMessage<T>>>,
  ) {
    guard.iter_mut().filter(|(id, _)| current_thread_id != **id).for_each(
      move |(&id, deque)| {
        deque.push_back(CoordinatorMessage::Oneshot {
          sender_id: current_thread_id,
          receiver_id: id,
          data: data.clone(),
        });
      },
    );
  }
}
