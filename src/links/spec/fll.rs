// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::monitoring::Monitoring;
use crate::links::Link;
use rand::RngExt;
use std::collections::{HashMap, VecDeque};
use std::sync::nonpoison::RwLock;
use std::sync::Arc;
use std::thread;
use std::thread::{sleep, JoinHandle};
use std::time::Duration;

struct FairLossLinkInner {
  participants: HashMap<usize, HashMap<usize, Vec<String>>>, // participant[] -> sender[] -> inbox[]
  queue: VecDeque<Envelope>,
  monitoring: Monitoring,
}

impl FairLossLinkInner {
  fn new() -> Self {
    Self {
      participants: Default::default(),
      queue: Default::default(),
      monitoring: Default::default(),
    }
  }

  fn send(&mut self, envelope: Envelope) {
    self.queue.push_back(envelope.clone());
    self.monitoring.record_send(envelope);
  }

  fn deliver(&mut self, envelope: Envelope) {
    if rand::rng().random_range(0f32..=1f32) < 0.45f32 {
      return;
    }

    let sender = self.participants.entry(envelope.receiver).or_default();
    let messages = sender.entry(envelope.sender).or_default();
    messages.push(envelope.message.clone());
    self.monitoring.record_deliver(envelope.clone());
  }
}

#[derive(Clone)]
pub struct FairLossLink {
  inner: Arc<RwLock<FairLossLinkInner>>,
}

impl FairLossLink {
  pub fn new() -> Self {
    Self {
      inner: Arc::new(RwLock::new(FairLossLinkInner::new())),
    }
  }

  pub fn delivered_count(&self) -> usize {
    self.inner.read().monitoring.traces.values().filter(|&&x| x).count()
  }
}

impl Link for FairLossLink {
  fn start(&self) -> JoinHandle<()> {
    let inner = self.inner.clone();
    thread::spawn(move || {
      loop {
        {
          let mut guard = inner.write();
          if let Some(envelope) = guard.queue.pop_front() {
            guard.deliver(envelope);
          }
        }

        sleep(Duration::from_millis(50));
      }
    })
  }

  fn send(&self, envelope: Envelope) {
    self.inner.write().send(envelope);
  }
}

#[test]
#[cfg(debug_assertions)]
fn test() {
  let fll = FairLossLink::new();
  fll.start();
  for i in 1..=100 {
    let fll = fll.clone();
    thread::spawn(move || {
      fll.send(Envelope::new(0, i, "hello!".to_string()));
    });
  }

  sleep(Duration::from_secs(5));
  assert!(fll.delivered_count() >= 50);
}
