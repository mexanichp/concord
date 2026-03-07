// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::sl::StubbornLink;
use crate::links::{Event, Link};
use std::collections::HashSet;
use std::io::Read;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::nonpoison::RwLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

struct PerfectLinkInner {
  sl: StubbornLink,
  delivered: HashSet<Envelope>,
  sender: Sender<Event>,
}

impl PerfectLinkInner {
  fn new(sender: Sender<Event>) -> (Self, Receiver<Event>) {
    let (tx, rx) = channel();
    let sl = StubbornLink::new(tx);
    (
      Self {
        sl,
        delivered: HashSet::new(),
        sender,
      },
      rx,
    )
  }

  fn deliver(&self, envelope: Envelope) {
    self.sender.send(Event::Delivered(envelope)).unwrap();
  }
}

pub struct PerfectLink {
  inner: Arc<RwLock<PerfectLinkInner>>,
  callback: Arc<Mutex<Option<Receiver<Event>>>>,
}

impl PerfectLink {
  pub fn new(sender: Sender<Event>) -> Self {
    let (pli, rx) = PerfectLinkInner::new(sender);
    Self {
      inner: Arc::new(RwLock::new(pli)),
      callback: Arc::new(Mutex::new(Some(rx))),
    }
  }
}

impl Link for PerfectLink {
  fn start(&self) -> JoinHandle<()> {
    let t1 = self.inner.read().sl.start();
    let inner = self.inner.clone();
    let receiver = self.callback.lock().unwrap().take().unwrap();
    let t2 = thread::spawn(move || {
      loop {
        if let Ok(event) = receiver.recv() {
          match event {
            Event::Delivered(envelope) => {
              if inner.read().delivered.contains(&envelope) {
                continue;
              }
              let mut lock = inner.write();
              if lock.delivered.contains(&envelope) {
                continue;
              }
              lock.delivered.insert(envelope.clone());
              lock.sender.send(Event::Delivered(envelope)).unwrap();
            }
          }
        };
      }
    });

    thread::spawn(move || {
      t1.join().unwrap();
      t2.join().unwrap();
    })
  }

  fn send(&self, envelope: Envelope) {
    self.inner.read().sl.send(envelope);
  }
}
/// **Abstract:**
/// The result of the execution clearly shows the problem of concurrency and causality.
///
/// While sender sends all the events sequentially, the replies (as well as the receiving participant)
/// may receive the messages out of order.
///
/// **Guarantees:**
/// Exactly once delivery through retries and idempotency.
///
/// **Example:**
/// ```
/// Messenger started
/// Delivered exactly once Envelope { sender: 2, receiver: 1, message: "Echo Participant 1 sends message 0" }
/// Delivered exactly once Envelope { sender: 2, receiver: 1, message: "Echo Participant 1 sends message 2" }
/// Delivered exactly once Envelope { sender: 2, receiver: 1, message: "Echo Participant 1 sends message 3" }
/// Delivered exactly once Envelope { sender: 2, receiver: 1, message: "Echo Participant 1 sends message 1" }
/// Delivered exactly once Envelope { sender: 2, receiver: 1, message: "Echo Participant 1 sends message 4" }
/// ```
#[test]
#[cfg(debug_assertions)]
pub fn testpl() {
  let (tx, rx) = channel();
  thread::spawn(move || {
    let (tx, receiver) = channel();
    thread::spawn(move || {
      loop {
        if let Ok(event) = receiver.recv() {
          match event {
            Event::Delivered(envelope) => {
              println!("Delivered exactly once {:?}", envelope);
            }
          }
        }
      }
    });
    let participant2 = PerfectLink::new(tx);
    participant2.start();
    loop {
      if let Ok(event) = rx.recv() {
        match event {
          Event::Delivered(envelope) => participant2.send(Envelope::new(
            envelope.receiver,
            envelope.sender,
            format!("Echo {}", envelope.message),
          )),
        }
      }
    }
  });

  let participant1 = PerfectLink::new(tx);
  participant1.start();
  thread::spawn(move || {
    println!("Messenger started");
    for i in 0..5 {
      let message = format!("Participant 1 sends message {i}");
      participant1.send(Envelope::new(1, 2, message));
    }
  });

  sleep(Duration::from_secs(5));
}
