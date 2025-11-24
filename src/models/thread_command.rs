// SPDX-License-Identifier: GPL-3.0+

use std::sync::Arc;

#[derive(Debug)]
pub enum ThreadCommand {
  EXIT,
  DATA(u64, String),
  BROADCAST(Arc<ThreadCommand>),
  SEND(u64, Arc<ThreadCommand>),
  KILL(u64),
}
