// SPDX-License-Identifier: GPL-3.0+

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Envelope {
  pub sender: usize,
  pub receiver: usize,
  pub message: String,
}

impl Envelope {
  pub fn new(sender: usize, receiver: usize, message: String) -> Self {
    Self {
      sender,
      receiver,
      message,
    }
  }
}
