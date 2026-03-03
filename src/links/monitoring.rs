// SPDX-License-Identifier: GPL-3.0+

use crate::links::envelope::Envelope;
use std::collections::HashMap;

#[derive(Default)]
pub struct Monitoring {
  pub traces: HashMap<Envelope, bool>,
}

impl Monitoring {
  pub fn record_send(&mut self, envelope: Envelope) {
    self.traces.entry(envelope).or_default();
  }

  pub fn record_deliver(&mut self, envelope: Envelope) {
    *self.traces.get_mut(&envelope).expect("The value must be sent") = true;
  }
}
