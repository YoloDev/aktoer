use std::{
  collections::VecDeque,
  future::Future,
  mem,
  pin::Pin,
  sync::atomic::Ordering,
  sync::Arc,
  sync::{
    atomic::{AtomicU16, AtomicU64},
    Weak,
  },
  task::Context as TaskContext,
  task::{Poll, Waker},
  thread,
};

use crate::SocketType;
use crossbeam_channel::{Receiver, Sender, TryRecvError};
use crossbeam_utils::atomic::AtomicCell;
use futures::future::FusedFuture;
use indexmap::IndexMap;
use static_assertions::assert_impl_all;
use tracing::{event, span, Level};

pub(crate) enum AsyncResult<T, E> {
  Pending,
  Ok(T),
  Err(E),
}

impl<T, E> From<Result<T, E>> for AsyncResult<T, E> {
  fn from(v: Result<T, E>) -> Self {
    match v {
      Ok(v) => Self::Ok(v),
      Err(e) => Self::Err(e),
    }
  }
}

impl<T, E> Default for AsyncResult<T, E> {
  #[inline]
  fn default() -> Self {
    Self::Pending
  }
}

struct AsyncOpInner<T, E> {
  result: AtomicCell<AsyncResult<T, E>>,
  waker: AtomicCell<Option<Waker>>,
}

pub(crate) struct AsyncOp<T, E> {
  id: u64,
  op: Arc<AsyncOpInner<T, E>>,
}

impl<T, E> Clone for AsyncOp<T, E> {
  #[inline]
  fn clone(&self) -> Self {
    Self {
      id: self.id,
      op: self.op.clone(),
    }
  }
}

pub(crate) struct AsyncOpRef<T, E> {
  id: u64,
  op: Weak<AsyncOpInner<T, E>>,
}

impl<T, E> AsyncOp<T, E> {
  pub(crate) fn new() -> Self {
    static NEXT_OP_ID: AtomicU64 = AtomicU64::new(1);

    Self {
      id: NEXT_OP_ID.fetch_add(1, Ordering::SeqCst),
      op: Arc::new(AsyncOpInner {
        result: AtomicCell::new(AsyncResult::Pending),
        waker: AtomicCell::new(None),
      }),
    }
  }

  pub(crate) fn poll(&self, cx: &mut TaskContext<'_>) -> Poll<Result<T, E>> {
    match self.op.result.take() {
      AsyncResult::Ok(v) => Poll::Ready(Ok(v)),
      AsyncResult::Err(e) => Poll::Ready(Err(e)),
      AsyncResult::Pending => {
        self.op.waker.store(Some(cx.waker().clone()));
        Poll::Pending
      }
    }
  }

  fn complete(&self, result: Result<T, E>) {
    let op = self.op.as_ref();
    op.result.store(result.into());
    if let Some(waker) = op.waker.take() {
      waker.wake()
    }
  }

  pub(crate) fn id(&self) -> u64 {
    self.id
  }

  fn downgrade(&self) -> AsyncOpRef<T, E> {
    AsyncOpRef {
      id: self.id,
      op: Arc::downgrade(&self.op),
    }
  }
}

impl<T, E> AsyncOpRef<T, E> {
  fn upgrade(&self) -> Option<AsyncOp<T, E>> {
    match self.op.upgrade() {
      None => None,
      Some(op) => Some(AsyncOp { id: self.id, op }),
    }
  }

  #[inline]
  fn strong_count(&self) -> usize {
    self.op.strong_count()
  }
}

pub(crate) struct AsyncTask<T, E, I> {
  op: AsyncOp<T, E>,
  input: Box<I>,
}

struct AsyncTaskRef<T, E, I> {
  op: AsyncOpRef<T, E>,
  input: Box<I>,
}

impl<T, E, I> AsyncTask<T, E, I> {
  pub(crate) fn new(input: I) -> Self {
    Self {
      op: AsyncOp::new(),
      input: Box::new(input),
    }
  }

  #[inline]
  fn op(&self) -> AsyncOp<T, E> {
    self.op.clone()
  }

  #[inline]
  fn complete(&self, result: Result<T, E>) {
    self.op.complete(result)
  }
}

impl<T, E, I> From<AsyncTask<T, E, I>> for AsyncOp<T, E> {
  #[inline]
  fn from(task: AsyncTask<T, E, I>) -> Self {
    task.op
  }
}

pub(crate) enum AsyncStateMachine<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  Unsent(&'a Context, AsyncTask<T, E, I>, F),
  Sent(&'a Context, AsyncOp<T, E>),
  Completed,
}

pub(crate) struct ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  state: AsyncStateMachine<'a, T, E, I, F>,
}

impl<'a, T, E, I, F> ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  #[inline]
  pub(crate) fn new(context: &'a Context, input: I, factory: F) -> Self {
    Self {
      state: AsyncStateMachine::Unsent(context, AsyncTask::new(input), factory),
    }
  }
}

impl<'a, T, E, I, F> Future for ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  type Output = Result<T, E>;

  fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<Self::Output> {
    let state = &mut self.get_mut().state;
    let op = match state {
      AsyncStateMachine::Unsent(context, task, _) => {
        let context: &'a Context = context;
        let op = task.op();
        match mem::replace(state, AsyncStateMachine::Sent(context, op)) {
          AsyncStateMachine::Unsent(ctx, task, factory) => {
            let task = factory(task);

            ctx.0.sender.send(task).unwrap();
          }
          _ => unreachable!(),
        }

        match state {
          AsyncStateMachine::Sent(_, op) => op,
          _ => unreachable!(),
        }
      }
      AsyncStateMachine::Sent(_, op) => op,
      AsyncStateMachine::Completed => panic!("Poll on completed async state machine"),
    };

    match op.poll(cx) {
      Poll::Pending => Poll::Pending,
      Poll::Ready(v) => {
        *state = AsyncStateMachine::Completed;
        Poll::Ready(v)
      }
    }
  }
}

impl<'a, T, E, I, F> FusedFuture for ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  fn is_terminated(&self) -> bool {
    match &self.state {
      AsyncStateMachine::Completed => true,
      _ => false,
    }
  }
}

impl<'a, T, E, I, F> Drop for ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  fn drop(&mut self) {
    match &self.state {
      AsyncStateMachine::Sent(context, op) => {
        context.0.sender.send(ReactorTask::Cancel(op.id())).unwrap()
      }
      _ => (),
    }
  }
}

type Messages = Vec<Vec<u8>>;

struct Socket {
  id: u32,
  socket: Arc<zmq::Socket>,
  read_task: Option<AsyncTaskRef<Messages, zmq::Error, ()>>,
  write_tasks: VecDeque<AsyncTaskRef<(), zmq::Error, Messages>>,
}

impl Socket {
  fn new(id: u32, socket: zmq::Socket) -> Self {
    Self {
      id,
      socket: Arc::new(socket),
      read_task: None,
      write_tasks: VecDeque::new(),
    }
  }

  fn is_sys(&self) -> bool {
    self.id == 0
  }

  fn read_task(&mut self) -> Option<AsyncTask<Messages, zmq::Error, ()>> {
    match self.read_task.take() {
      None => None,
      Some(task) => match task.op.upgrade() {
        None => None,
        Some(op) => Some(AsyncTask {
          op,
          input: task.input,
        }),
      },
    }
  }

  fn has_read_task(&self) -> bool {
    match &self.read_task {
      None => false,
      Some(v) => v.op.strong_count() > 0,
    }
  }

  fn next_write_task(&mut self) -> Option<AsyncTask<(), zmq::Error, Messages>> {
    while let Some(task) = self.write_tasks.pop_front() {
      match task.op.upgrade() {
        None => continue,
        Some(op) => {
          return Some(AsyncTask {
            op,
            input: task.input,
          })
        }
      }
    }

    None
  }

  fn has_write_task(&self) -> bool {
    self.write_tasks.iter().any(|t| t.op.strong_count() > 0)
  }

  fn is_reading(&self) -> bool {
    self.is_sys() || self.has_read_task()
  }

  fn is_writing(&self) -> bool {
    self.has_write_task()
  }

  fn cancel_task(&mut self, id: u64) -> bool {
    match &self.read_task {
      Some(t) if t.op.id == id => {
        self.read_task = None;
        return true;
      }
      _ => (),
    }

    let start_len = self.write_tasks.len();
    self.write_tasks.retain(|t| t.op.id != id);
    self.write_tasks.len() < start_len
  }

  fn as_poll_item(&self) -> (Arc<zmq::Socket>, zmq::PollEvents) {
    let mut events = zmq::PollEvents::empty();
    events.set(zmq::PollEvents::POLLIN, self.is_reading());
    events.set(zmq::PollEvents::POLLOUT, self.is_writing());
    (self.socket.clone(), events)
  }
}

struct Reactor {
  zmq_context: zmq::Context,
  sockets: IndexMap<u32, Socket>,
  receiver: Receiver<ReactorTask>,
  next_socket_id: u32,
}

pub(crate) enum ReactorTask {
  /// Cancels a pending read or write task (sent by dropping the future).
  Cancel(u64),
  Create(AsyncTask<u32, zmq::Error, SocketType>),
  Destroy(AsyncTask<(), zmq::Error, u32>),
  Bind(AsyncTask<(), zmq::Error, (u32, String)>),
  Connect(AsyncTask<(), zmq::Error, (u32, String)>),
  Send(AsyncTask<(), zmq::Error, (u32, Messages)>),
  Recv(AsyncTask<Messages, zmq::Error, u32>),
}

impl ReactorTask {
  pub fn id(&self) -> u64 {
    match self {
      ReactorTask::Cancel(id) => *id,
      ReactorTask::Create(task) => task.op.id(),
      ReactorTask::Destroy(task) => task.op.id(),
      ReactorTask::Bind(task) => task.op.id(),
      ReactorTask::Connect(task) => task.op.id(),
      ReactorTask::Send(task) => task.op.id(),
      ReactorTask::Recv(task) => task.op.id(),
    }
  }

  pub fn kind(&self) -> &'static str {
    match self {
      ReactorTask::Cancel(_) => "cancel",
      ReactorTask::Create(_) => "create",
      ReactorTask::Destroy(_) => "destroy",
      ReactorTask::Bind(_) => "bind",
      ReactorTask::Connect(_) => "connect",
      ReactorTask::Send(_) => "send",
      ReactorTask::Recv(_) => "recv",
    }
  }
}

impl Reactor {
  fn create(&mut self, socket_type: SocketType) -> Result<u32, zmq::Error> {
    let socket = self.zmq_context.socket(socket_type)?;
    let socket_id = self.next_socket_id;
    self.next_socket_id += 1;

    self
      .sockets
      .insert(socket_id, Socket::new(socket_id, socket));
    Ok(socket_id)
  }

  fn destroy(&mut self, id: u32) -> Result<(), zmq::Error> {
    self.sockets.remove(&id);
    Ok(())
  }

  fn bind(&mut self, (id, endpoint): (u32, String)) -> Result<(), zmq::Error> {
    let socket: &Socket = &self.sockets[&id];
    socket.socket.bind(&endpoint)
  }

  fn connect(&mut self, (id, endpoint): (u32, String)) -> Result<(), zmq::Error> {
    let socket: &Socket = &self.sockets[&id];
    socket.socket.connect(&endpoint)
  }

  fn start_send(&mut self, task: AsyncTask<(), zmq::Error, (u32, Messages)>) {
    let (id, messages) = *task.input;
    let socket: &mut Socket = &mut self.sockets[&id];
    socket.write_tasks.push_back(AsyncTaskRef {
      op: task.op.downgrade(),
      input: Box::new(messages),
    });
  }

  fn start_recv(&mut self, task: AsyncTask<Messages, zmq::Error, u32>) {
    let id = *task.input;
    let socket: &mut Socket = &mut self.sockets[&id];
    if socket.has_read_task() {
      task.complete(Err(zmq::Error::EBUSY));
    } else {
      socket.read_task = Some(AsyncTaskRef {
        op: task.op.downgrade(),
        input: Box::new(()),
      });
    }
  }

  fn cancel_task(&mut self, id: u64) {
    for s in self.sockets.values_mut() {
      if s.cancel_task(id) {
        event!(target: "zmq", Level::DEBUG, "task cancelled");
        return;
      }
    }

    event!(target: "zmq", Level::DEBUG, "cancelled task already completed");
  }
}

impl Reactor {
  fn run(&mut self, task: ReactorTask) {
    fn run_task<I, T>(
      reactor: &mut Reactor,
      task: AsyncTask<T, zmq::Error, I>,
      f: impl FnOnce(&mut Reactor, I) -> Result<T, zmq::Error>,
    ) {
      event!(target: "zmq", Level::DEBUG, "running task");
      let result = f(reactor, *task.input);
      task.op.complete(result);
    }

    match task {
      ReactorTask::Cancel(id) => self.cancel_task(id),
      ReactorTask::Create(task) => run_task(self, task, Reactor::create),
      ReactorTask::Destroy(task) => run_task(self, task, Reactor::destroy),
      ReactorTask::Bind(task) => run_task(self, task, Reactor::bind),
      ReactorTask::Connect(task) => run_task(self, task, Reactor::connect),
      ReactorTask::Send(task) => self.start_send(task),
      ReactorTask::Recv(task) => self.start_recv(task),
    }
  }
}

pub(crate) struct ContextInner {
  pub(crate) sender: Sender<ReactorTask>,
}

#[derive(Clone)]
pub struct Context(pub(crate) Arc<ContextInner>);

assert_impl_all!(Context: Send, Sync);

impl Context {
  pub fn new() -> Self {
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
                    let result = socket.socket.send_multipart(*task.input, 0);
                    task.op.complete(result);
                  }
                }
              }
            }
          }
        })
    };

    let inner = ContextInner {
      sender: channel_sender,
    };

    Context(Arc::new(inner))
  }
}
