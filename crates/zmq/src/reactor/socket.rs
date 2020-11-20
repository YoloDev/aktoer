use std::{collections::VecDeque, sync::Arc};

use super::{future::AsyncTaskRef, AsyncTask};

pub(crate) type Messages = Vec<Vec<u8>>;

pub(crate) struct Socket {
  pub(super) id: u32,
  pub(super) socket: Arc<zmq::Socket>,
  pub(super) read_task: Option<AsyncTaskRef<Messages, zmq::Error, ()>>,
  pub(super) write_tasks: VecDeque<AsyncTaskRef<(), zmq::Error, Messages>>,
}

impl Socket {
  pub(crate) fn new(id: u32, socket: zmq::Socket) -> Self {
    Self {
      id,
      socket: Arc::new(socket),
      read_task: None,
      write_tasks: VecDeque::new(),
    }
  }

  pub(crate) fn is_sys(&self) -> bool {
    self.id == 0
  }

  pub(crate) fn read_task(&mut self) -> Option<AsyncTask<Messages, zmq::Error, ()>> {
    self.read_task.take().and_then(AsyncTaskRef::upgrade)
  }

  pub(crate) fn has_read_task(&self) -> bool {
    match &self.read_task {
      None => false,
      Some(v) => v.strong_count() > 0,
    }
  }

  pub(crate) fn next_write_task(&mut self) -> Option<AsyncTask<(), zmq::Error, Messages>> {
    while let Some(task) = self.write_tasks.pop_front() {
      match task.upgrade() {
        None => continue,
        Some(t) => return Some(t),
      }
    }

    None
  }

  pub(crate) fn has_write_task(&self) -> bool {
    self.write_tasks.iter().any(|t| t.strong_count() > 0)
  }

  pub(crate) fn is_reading(&self) -> bool {
    self.is_sys() || self.has_read_task()
  }

  pub(crate) fn is_writing(&self) -> bool {
    self.has_write_task()
  }

  pub(crate) fn cancel_task(&mut self, id: u64) -> bool {
    match &self.read_task {
      Some(t) if t.id() == id => {
        self.read_task = None;
        return true;
      }
      _ => (),
    }

    let start_len = self.write_tasks.len();
    self.write_tasks.retain(|t| t.id() != id);
    self.write_tasks.len() < start_len
  }

  pub(crate) fn as_poll_item(&self) -> (Arc<zmq::Socket>, zmq::PollEvents) {
    let mut events = zmq::PollEvents::empty();
    events.set(zmq::PollEvents::POLLIN, self.is_reading());
    events.set(zmq::PollEvents::POLLOUT, self.is_writing());
    (self.socket.clone(), events)
  }
}
