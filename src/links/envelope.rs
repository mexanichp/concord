// SPDX-License-Identifier: GPL-3.0+

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
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
