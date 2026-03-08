// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use crate::links::spec::sl::StubbornLink;
use crate::links::{Event, Link};
use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::nonpoison::Mutex;
use std::sync::{Arc, LazyLock};
use std::thread;
use std::thread::JoinHandle;
use std::time::Duration;

struct LoggedPerfectLinkInner {
  sl: StubbornLink,
  sender: Sender<Event>,
  delivered: HashSet<Envelope>,
}

impl LoggedPerfectLinkInner {
  const DELIVERED_FILE_NAME: &str = "delivered.txt";
  fn delivered_file_path() -> &'static PathBuf {
    static PATH: LazyLock<PathBuf> = LazyLock::new(move || {
      let mut path = std::env::current_exe()
        .unwrap()
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf();
      path.push(LoggedPerfectLinkInner::DELIVERED_FILE_NAME);
      path
    });

    &PATH
  }

  fn new(sender: Sender<Event>) -> (Self, Receiver<Event>) {
    let (tx, rx) = channel();
    (
      Self {
        sl: StubbornLink::new(tx),
        sender,
        delivered: HashSet::new(),
      },
      rx,
    )
  }

  fn deliver(&self, envelope: Envelope) {
    self.sender.send(Event::Delivered(envelope)).unwrap();
  }

  fn save(&self) {
    let file = File::create(Self::delivered_file_path()).unwrap();
    serde_json::to_writer(file, &self.delivered).unwrap();
  }

  fn restore(&mut self) {
    let file = OpenOptions::new()
      .create(true)
      .write(true)
      .read(true)
      .open(Self::delivered_file_path())
      .unwrap();
    let reader = BufReader::new(file);
    self.delivered = serde_json::from_reader(reader).unwrap_or_default();
  }
}

#[derive(Clone)]
pub struct LoggedPerfectLink {
  inner: Arc<Mutex<LoggedPerfectLinkInner>>,
  callback: Arc<Mutex<Option<Receiver<Event>>>>,
}

impl LoggedPerfectLink {
  fn new(sender: Sender<Event>) -> Self {
    let (inner, rx) = LoggedPerfectLinkInner::new(sender);
    Self {
      inner: Arc::new(Mutex::new(inner)),
      callback: Arc::new(Mutex::new(Some(rx))),
    }
  }
}

impl Link for LoggedPerfectLink {
  fn start(&self) -> JoinHandle<()> {
    let t1 = self.inner.lock().sl.start();
    let receiver = self.callback.lock().take().unwrap();
    let inner = self.inner.clone();
    inner.lock().restore();
    let t2 = thread::spawn(move || {
      loop {
        if let Ok(event) = receiver.recv() {
          match event {
            Event::Delivered(envelope) => {
              let mut guard = inner.lock();
              if guard.delivered.contains(&envelope) {
                continue;
              }

              guard.delivered.insert(envelope.clone());
              guard.save(); // this becomes the source of truth
              guard.deliver(envelope.clone()) // this becomes best-effort try to notify
            }
          }
        }
      }
    });

    thread::spawn(move || {
      t1.join().unwrap();
      t2.join().unwrap();
    })
  }

  fn send(&self, envelope: Envelope) {
    self.inner.lock().sl.send(envelope);
  }
}

#[test]
fn test() {
  let (tx, rx) = channel();

  let lpl = LoggedPerfectLink::new(tx);
  lpl.start();

  thread::spawn(move || {
    loop {
      if let Ok(event) = rx.recv() {
        match event {
          Event::Delivered(envelope) => {
            println!("Got {envelope:?}");
          }
        }
      }
    }
  });

  for i in 0..10 {
    let lpl = lpl.clone();
    thread::spawn(move || {
      lpl.send(Envelope::new(0, i, format!("Hello {i}")));
    });
  }

  thread::sleep(Duration::from_secs(5));
}
