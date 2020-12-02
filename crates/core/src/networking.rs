use std::{collections::HashMap, sync::Arc};

use aktoer_zmq::{Context, Socket};
use async_std::sync::{Mutex, RwLock};
use chrono::{DateTime, Utc};

use crate::id::GatewayAddress;

pub(crate) struct Connection {
  id: GatewayAddress,
  socket: Socket,
}

#[derive(Default)]
struct ConnectionManagerInner {
  context: Context,
  connections: HashMap<GatewayAddress, ConnectionEntry>,
}

#[derive(Default)]
pub(crate) struct ConnectionManager(Arc<Mutex<ConnectionManagerInner>>);

impl ConnectionManager {
  pub fn new() -> Self {
    Self::default()
  }

  pub async fn len(&self) -> usize {
    let lock = self.0.lock().await;
    lock.connections.len()
  }

  pub async fn get_connected_addresses(&self) -> Vec<GatewayAddress> {
    let lock = self.0.lock().await;
    lock
      .connections
      .keys()
      .into_iter()
      .cloned()
      .collect::<Vec<_>>()
  }

  pub async fn close(&mut self, endpoint: &GatewayAddress) {
    let mut lock = self.0.lock().await;
    lock.connections.remove(endpoint);
  }
}

struct ConnectionEntry {
  address: GatewayAddress,
  connections: Box<[Connection]>,
  last_failure: Option<DateTime<Utc>>,
}
