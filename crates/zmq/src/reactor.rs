mod future;
mod socket;
mod task;

pub(crate) use self::future::{AsyncTask, ZmqFuture};
pub(crate) use self::task::ReactorTask;

use std::{fmt, sync::atomic::AtomicU16, sync::atomic::Ordering, sync::Arc, thread};

use crossbeam_channel::{Receiver, Sender, TryRecvError};
use indexmap::IndexMap;
use socket::Socket;
use tracing::{event, span, Level};

struct ReactorRefInner {
  id: u16,
  sender: Sender<ReactorTask>,
}

#[derive(Clone)]
pub(crate) struct ReactorRef(Arc<ReactorRefInner>);

impl fmt::Debug for ReactorRef {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_tuple("ReactorRef").field(&self.0.id).finish()
  }
}

impl ReactorRef {
  #[inline]
  pub(crate) fn send(&self, task: ReactorTask) -> Result<(), zmq::Error> {
    self.0.sender.send(task).map_err(|_| zmq::Error::EMTHREAD)
  }
}

struct Reactor {
  zmq_context: zmq::Context,
  sockets: IndexMap<u32, Socket>,
  receiver: Receiver<ReactorTask>,
  next_socket_id: u32,
}

impl Reactor {
  fn start_new() -> ReactorRef {
    use std::fmt::Write;
    static NEXT_CTX_ID: AtomicU16 = AtomicU16::new(1);

    let id = NEXT_CTX_ID.fetch_add(1, Ordering::SeqCst);
    let ctx_span = span!(target: "zmq", Level::DEBUG, "ctx", id);
    let mut inproc_addr = String::with_capacity("inproc://aktoer/reactor/".len() + 16);
    write!(inproc_addr, "inproc://aktoer/reactor/{}", id).unwrap();
    let context = zmq::Context::new();

    let (channel_sender, channel_receiver) = crossbeam_channel::unbounded::<ReactorTask>();
    let (reactor_sender, reactor_receiver) = crossbeam_channel::bounded::<ReactorTask>(0);

    let thread_socket = context.socket(zmq::SocketType::PAIR).unwrap();
    thread_socket.bind(&inproc_addr).unwrap();

    let _ = {
      let context = context.clone();
      let span = ctx_span.clone();

      thread::Builder::new()
        .name(format!("zmq-chan-{}", id))
        .spawn(move || {
          let span = span!(target: "zmq", parent: &span, Level::DEBUG, "chan");
          let _enter = span.enter();

          let thread_socket = context.socket(zmq::SocketType::PAIR).unwrap();
          thread_socket.connect(&inproc_addr).unwrap();

          loop {
            match channel_receiver.recv() {
              Ok(task) => {
                // TODO: Armortize?
                event!(target: "zmq", Level::DEBUG, task.kind = task.kind(), task.id = task.id(), "received task");
                thread_socket.send(zmq::Message::new(), 0).unwrap();
                reactor_sender.send(task).unwrap();
              }
              Err(_) => {
                thread_socket.send(zmq::Message::new(), 0).unwrap();
                break;
              }
            }
          }
        })
    };

    let _ = {
      let context = context.clone();
      let span = ctx_span;

      thread::Builder::new()
        .name(format!("zmq-reactor-{}", id))
        .spawn(move || {
          let span = span!(target: "zmq", parent: &span, Level::DEBUG, "reactor");
          let _enter = span.enter();

          let mut reactor = Reactor {
            zmq_context: context,
            sockets: IndexMap::new(),
            receiver: reactor_receiver,
            next_socket_id: 1,
          };

          let thread_socket = Socket::new(0, thread_socket);
          debug_assert_eq!(thread_socket.id, 0);
          reactor.sockets.insert(thread_socket.id, thread_socket);

          let mut sockets = Vec::with_capacity(reactor.sockets.len());

          'outer: loop {
            sockets.clear();
            sockets.extend(reactor.sockets.values().map(|s| s.as_poll_item()));

            let mut listeners_len = 0;
            let mut listeners = sockets
              .iter()
              .map(|s| {
                if s.1 != zmq::PollEvents::empty() {
                  listeners_len += 1;
                }
                s.0.as_poll_item(s.1)
              })
              .collect::<Vec<_>>();

            event!(target: "zmq", Level::DEBUG, listeners.len = listeners_len, "start poll");
            zmq::poll(&mut listeners, -1).unwrap();
            event!(target: "zmq", Level::DEBUG, "poll completed");
            for (index, item) in listeners.iter().enumerate() {
              let socket_id = *reactor.sockets.get_index(index).unwrap().0;
              if socket_id == 0 {
                if item.is_readable() {
                  event!(target: "zmq", Level::DEBUG, "system socket is readable");
                  match reactor.receiver.try_recv() {
                    Err(TryRecvError::Empty) => {
                      event!(target: "zmq", Level::DEBUG, "receiver does not have any messages");
                    }
                    Err(TryRecvError::Disconnected) => {
                      event!(target: "zmq", Level::DEBUG, "receiver is disconnected");
                      break 'outer;
                    }
                    Ok(task) => {
                      let kind = task.kind();
                      let span = span!(target: "zmq", Level::DEBUG, "task", task.kind = kind, task.id = task.id());
                      let _enter = span.enter();
                      // consume message
                      reactor.sockets[&socket_id].socket.recv_msg(0).unwrap();
                      reactor.run(task);

                      // the world changes at this point - so listeners might no longer be valid
                      break;
                    }
                  }
                }
              } else {
                let socket: &mut Socket = &mut reactor.sockets[&socket_id];
                if item.is_readable() {
                  event!(target: "zmq", Level::DEBUG, socket.id = socket.id, "socket readable");
                  if let Some(task) = socket.read_task() {
                    let result = socket.socket.recv_multipart(0);
                    task.complete(result);
                  }
                }

                if item.is_writable() {
                  event!(target: "zmq", Level::DEBUG, socket.id = socket.id, "socket writable");
                  if let Some(task) = socket.next_write_task() {
                    let op = task.op();
                    let result = socket.socket.send_multipart(*task.input, 0);
                    op.complete(result);
                  }
                }
              }
            }
          }
        })
    };

    let inner = ReactorRefInner {
      id,
      sender: channel_sender,
    };

    ReactorRef(Arc::new(inner))
  }
}

impl ReactorRef {
  #[inline]
  pub(crate) fn new() -> Self {
    Reactor::start_new()
  }
}
