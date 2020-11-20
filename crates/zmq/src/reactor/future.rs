use std::{
  future::Future,
  mem,
  pin::Pin,
  sync::atomic::Ordering,
  sync::Arc,
  sync::{atomic::AtomicU64, Weak},
  task::Context,
  task::{Poll, Waker},
};

use crossbeam_utils::atomic::AtomicCell;
use futures::future::FusedFuture;

use super::{ReactorRef, ReactorTask};

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

pub(crate) struct AsyncOpInner<T, E> {
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

  pub(crate) fn poll(&self, cx: &mut Context<'_>) -> Poll<Result<T, E>> {
    match self.op.result.take() {
      AsyncResult::Ok(v) => Poll::Ready(Ok(v)),
      AsyncResult::Err(e) => Poll::Ready(Err(e)),
      AsyncResult::Pending => {
        self.op.waker.store(Some(cx.waker().clone()));
        Poll::Pending
      }
    }
  }

  pub(super) fn complete(&self, result: Result<T, E>) {
    let op = self.op.as_ref();
    op.result.store(result.into());
    if let Some(waker) = op.waker.take() {
      waker.wake()
    }
  }

  pub(crate) fn id(&self) -> u64 {
    self.id
  }

  pub(crate) fn downgrade(&self) -> AsyncOpRef<T, E> {
    AsyncOpRef {
      id: self.id,
      op: Arc::downgrade(&self.op),
    }
  }

  // pub(crate) fn into_task<I>(self, input: Box<I>) -> AsyncTask<T, E, I> {
  //   AsyncTask { op: self, input }
  // }
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

  pub(crate) fn into_task<I>(self, input: Box<I>) -> AsyncTaskRef<T, E, I> {
    AsyncTaskRef { op: self, input }
  }
}

pub(crate) struct AsyncTask<T, E, I> {
  op: AsyncOp<T, E>,
  pub(super) input: Box<I>,
}

pub(crate) struct AsyncTaskRef<T, E, I> {
  op: AsyncOpRef<T, E>,
  input: Box<I>,
}

impl<T, E, I> AsyncTaskRef<T, E, I> {
  #[inline]
  pub(crate) fn id(&self) -> u64 {
    self.op.id
  }

  #[inline]
  pub(crate) fn strong_count(&self) -> usize {
    self.op.strong_count()
  }

  pub(crate) fn upgrade(self) -> Option<AsyncTask<T, E, I>> {
    match self.op.upgrade() {
      None => None,
      Some(op) => Some(AsyncTask {
        op,
        input: self.input,
      }),
    }
  }
}

impl<T, E, I> AsyncTask<T, E, I> {
  pub(crate) fn new(input: I) -> Self {
    Self {
      op: AsyncOp::new(),
      input: Box::new(input),
    }
  }

  #[inline]
  pub(crate) fn id(&self) -> u64 {
    self.op.id
  }

  #[inline]
  pub(crate) fn op(&self) -> AsyncOp<T, E> {
    self.op.clone()
  }

  #[inline]
  pub(super) fn complete(&self, result: Result<T, E>) {
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
  Unsent(&'a ReactorRef, AsyncTask<T, E, I>, F),
  Sent(&'a ReactorRef, AsyncOp<T, E>),
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
  pub(crate) fn new(reactor: &'a ReactorRef, input: I, factory: F) -> Self {
    Self {
      state: AsyncStateMachine::Unsent(reactor, AsyncTask::new(input), factory),
    }
  }
}

impl<'a, T, E, I, F> Future for ZmqFuture<'a, T, E, I, F>
where
  F: Unpin,
  F: FnOnce(AsyncTask<T, E, I>) -> ReactorTask,
{
  type Output = Result<T, E>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    let state = &mut self.get_mut().state;
    let op = match state {
      AsyncStateMachine::Unsent(reactor, task, _) => {
        let reactor: &'a ReactorRef = reactor;
        let op = task.op();
        match mem::replace(state, AsyncStateMachine::Sent(reactor, op)) {
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
      AsyncStateMachine::Sent(reactor, op) => reactor.send(ReactorTask::Cancel(op.id())).unwrap(),
      _ => (),
    }
  }
}
