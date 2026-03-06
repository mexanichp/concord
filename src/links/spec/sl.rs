// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::fll::FairLossLink;
use crate::links::{Event, Link};
use std::collections::VecDeque;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::nonpoison::RwLock;
use std::sync::{Arc, Mutex};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct StubbornLinkInner {
  fll: FairLossLink,
  retransmit: VecDeque<Envelope>,
  sender: Sender<Event>,
}

impl StubbornLinkInner {
  fn new(sender: Sender<Event>) -> (Self, Receiver<Event>) {
    let (tx, rx) = channel();
    let fll = FairLossLink::new(tx);
    (
      Self {
        fll,
        retransmit: VecDeque::new(),
        sender,
      },
      rx,
    )
  }

  fn deliver(&self, envelope: Envelope) {
    match self.sender.send(Event::Delivered(envelope.clone())) {
      Ok(_) => {
        println!("Sent {envelope:?}")
      }
      Err(err) => {
        println!("Error {:?}", err.0)
      }
    };
  }
}

#[derive(Clone)]
pub struct StubbornLink {
  inner: Arc<RwLock<StubbornLinkInner>>,
  callback: Arc<Mutex<Option<Receiver<Event>>>>,
}

impl StubbornLink {
  pub fn new(sender: Sender<Event>) -> Self {
    let (sl, rx) = StubbornLinkInner::new(sender);
    Self {
      inner: Arc::new(RwLock::new(sl)),
      callback: Arc::new(Mutex::new(Some(rx))),
    }
  }

  pub fn delivered_count(&self) -> usize {
    self.inner.read().fll.delivered_count()
  }
}

impl Link for StubbornLink {
  fn start(&self) -> JoinHandle<()> {
    let inner = self.inner.clone();
    let t1 = inner.read().fll.start();
    let receiver = self.callback.lock().unwrap().take().unwrap();
    let t2 = thread::spawn(move || {
      let rec_inner = inner.clone();
      thread::spawn(move || {
        loop {
          match receiver.recv() {
            Ok(event) => match event {
              Event::Delivered(envelope) => rec_inner.read().deliver(envelope),
            },
            Err(_) => {}
          };
        }
      });
      loop {
        {
          let envelope = inner.write().retransmit.pop_front();
          if let Some(envelope) = envelope {
            inner.read().fll.send(envelope.clone());
            inner.write().retransmit.push_back(envelope);
          }
        }

        thread::sleep(Duration::from_millis(50));
      }
    });

    thread::spawn(move || {
      t1.join().unwrap();
      t2.join().unwrap();
    })
  }

  fn send(&self, envelope: Envelope) {
    self.inner.read().fll.send(envelope.clone());
    self.inner.write().retransmit.push_back(envelope);
  }
}

#[test]
#[cfg(debug_assertions)]
fn test() {
  let (tx, rx) = channel();
  let sl = StubbornLink::new(tx);
  sl.start();
  for i in 0..100 {
    let sl = sl.clone();
    thread::spawn(move || {
      sl.send(Envelope::new(0, i, format!("Hello {}", i)));
    });
  }

  while sl.delivered_count() != 100 {
    thread::sleep(Duration::from_millis(100));
  }
}
