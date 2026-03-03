// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::fll::FairLossLink;
use crate::links::Link;
use std::collections::VecDeque;
use std::sync::nonpoison::RwLock;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct StubbornLinkInner {
  fll: FairLossLink,
  retransmit: VecDeque<Envelope>,
}

impl StubbornLinkInner {
  fn new() -> Self {
    Self {
      fll: FairLossLink::new(),
      retransmit: VecDeque::new(),
    }
  }
}

#[derive(Clone)]
pub struct StubbornLink {
  inner: Arc<RwLock<StubbornLinkInner>>,
}

impl StubbornLink {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(StubbornLinkInner::new())),
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
    let t2 = thread::spawn(move || {
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
fn test() {
  let sl = StubbornLink::new();
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
