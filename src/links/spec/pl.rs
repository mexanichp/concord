// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::sl::StubbornLink;
use crate::links::{Event, Link};
use std::collections::HashSet;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::nonpoison::RwLock;
use std::sync::{mpsc, Arc};
use std::thread;
use std::thread::JoinHandle;

struct PerfectLinkInner {
  sl: StubbornLink,
  delivered: HashSet<Envelope>,
}

impl PerfectLinkInner {
  fn new() -> (Self, Receiver<Event>) {
    let (sl, sl_rx) = StubbornLink::new();
    (
      Self {
        sl,
        delivered: HashSet::new(),
      },
      sl_rx,
    )
  }
}

pub struct PerfectLink {
  inner: Arc<RwLock<PerfectLinkInner>>,
  sender: Sender<Event>,
}

impl PerfectLink {
  pub fn new(sender: Sender<Event>) -> Self {
    Self {
      inner: Arc::new(RwLock::new(PerfectLinkInner::new())),
      sender,
    }
  }
}

impl Link for PerfectLink {
  fn start(&self) -> JoinHandle<()> {
    let t1 = self.inner.read().sl.start();
    let (tx, rx) = mpsc::channel::<Event>();
    let t2 = thread::spawn(move || {});

    thread::spawn(move || {
      t1.join().unwrap();
      t2.join().unwrap();
    })
  }

  fn send(&self, envelope: Envelope) {
    todo!()
  }
}
