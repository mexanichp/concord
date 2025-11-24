// SPDX-License-Identifier: GPL-3.0+

pub enum SendMessage<T> {
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

pub enum ReplyMessage<T: Clone> {
  None,
  Oneshot { receiver_id: u64, data: T },
  Broadcast { data: T },
}
