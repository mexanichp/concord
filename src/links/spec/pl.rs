// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::sl::StubbornLink;
use crate::links::{Event, Link};
use std::collections::HashSet;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::nonpoison::RwLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
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

#[test]
#[cfg(debug_assertions)]
fn test() {
  let (tx, rx) = channel();
  thread::spawn(move || {
    loop {
      if let Ok(event) = rx.recv() {
        println!("Delivered exactly once {event:?}")
      } else {
        println!("Error occurred")
      }
    }
  });
  let pl = PerfectLink::new(tx);
  pl.start();

  pl.send(Envelope::new(0, 1, "Hello #1!".to_string()));
  pl.send(Envelope::new(0, 1, "Hello #1!".to_string()));
  pl.send(Envelope::new(0, 2, "Hello #2!".to_string()));

  thread::sleep(Duration::from_secs(5));
}
