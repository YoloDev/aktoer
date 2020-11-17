use crate::{
  reactor::AsyncTask,
  reactor::{ReactorTask, ZmqFuture},
  Context, SocketType,
};

use static_assertions::assert_impl_all;

pub struct Socket {
  context: Context,
  id: u32,
}

assert_impl_all!(Socket: Send, Sync);

impl Context {
  pub async fn socket(&self, socket_type: SocketType) -> Result<Socket, zmq::Error> {
    let id = ZmqFuture::new(self, socket_type, ReactorTask::Create).await?;
    Ok(Socket {
      context: self.clone(),
      id,
    })
  }
}

impl Drop for Socket {
  fn drop(&mut self) {
    self
      .context
      .0
      .sender
      .send(ReactorTask::Destroy(AsyncTask::new(self.id)))
      .unwrap()
  }
}

impl Socket {
  pub async fn bind(&mut self, address: &str) -> Result<(), zmq::Error> {
    ZmqFuture::new(&self.context, (self.id, address.into()), ReactorTask::Bind).await
  }

  pub async fn connect(&mut self, address: &str) -> Result<(), zmq::Error> {
    ZmqFuture::new(
      &self.context,
      (self.id, address.into()),
      ReactorTask::Connect,
    )
    .await
  }

  pub async fn send(&mut self, messages: Vec<Vec<u8>>) -> Result<(), zmq::Error> {
    ZmqFuture::new(&self.context, (self.id, messages), ReactorTask::Send).await
  }

  pub async fn recv(&mut self) -> Result<Vec<Vec<u8>>, zmq::Error> {
    ZmqFuture::new(&self.context, self.id, ReactorTask::Recv).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use futures::try_join;

  #[async_std::test]
  async fn test_create() {
    // use tracing::Level;
    // use std::io;
    // tracing_subscriber::FmtSubscriber::builder()
    //   .pretty()
    //   .with_ansi(true)
    //   .with_level(true)
    //   .with_max_level(Level::DEBUG)
    //   .with_writer(io::stderr)
    //   .init();

    let ctx = Context::new();
    let (mut s1, mut s2) =
      try_join!(ctx.socket(SocketType::REP), ctx.socket(SocketType::REQ)).unwrap();
    try_join!(
      s1.bind("tcp://127.0.0.1:8155"),
      s2.connect("tcp://127.0.0.1:8155")
    )
    .unwrap();

    s2.send(vec![b"HELLO".iter().copied().collect()])
      .await
      .unwrap();
    let msg = s1.recv().await.unwrap();

    assert_eq!(msg.len(), 1);
    assert_eq!(&msg[0], b"HELLO");
  }
}
