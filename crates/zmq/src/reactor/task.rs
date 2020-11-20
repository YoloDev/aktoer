use crate::SocketType;
use tracing::{event, Level};

use super::{
  socket::{Messages, Socket},
  AsyncTask, Reactor,
};

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
      ReactorTask::Create(task) => task.id(),
      ReactorTask::Destroy(task) => task.id(),
      ReactorTask::Bind(task) => task.id(),
      ReactorTask::Connect(task) => task.id(),
      ReactorTask::Send(task) => task.id(),
      ReactorTask::Recv(task) => task.id(),
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
    let op = task.op();
    let (id, messages) = *task.input;
    let socket: &mut Socket = &mut self.sockets[&id];
    socket
      .write_tasks
      .push_back(op.downgrade().into_task(Box::new(messages)));
  }

  fn start_recv(&mut self, task: AsyncTask<Messages, zmq::Error, u32>) {
    let id = *task.input;
    let socket: &mut Socket = &mut self.sockets[&id];
    if socket.has_read_task() {
      task.complete(Err(zmq::Error::EBUSY));
    } else {
      socket.read_task = Some(task.op().downgrade().into_task(Box::new(())));
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
  pub(super) fn run(&mut self, task: ReactorTask) {
    fn run_task<I, T>(
      reactor: &mut Reactor,
      task: AsyncTask<T, zmq::Error, I>,
      f: impl FnOnce(&mut Reactor, I) -> Result<T, zmq::Error>,
    ) {
      event!(target: "zmq", Level::DEBUG, "running task");
      let op = task.op();
      let result = f(reactor, *task.input);
      op.complete(result);
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
