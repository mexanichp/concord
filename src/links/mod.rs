// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use std::thread::JoinHandle;

pub mod envelope;
pub mod monitoring;
pub mod spec;

pub trait Link {
  fn start(&self) -> JoinHandle<()>;
  fn send(&self, envelope: Envelope);
}

pub enum Event {
  Delivered(Envelope),
}
