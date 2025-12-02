// SPDX-License-Identifier: GPL-3.0+

#[derive(Clone)]
pub enum CoordinatorMessage<T> {
  None,
  Broadcast {
    sender_id: u64,
    data: T,
  },
  Oneshot {
    sender_id: u64,
    receiver_id: u64,
    data: T,
  },
}
