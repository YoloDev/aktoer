use std::{
  collections::{HashMap, HashSet},
  mem,
  sync::Arc,
};

use async_std::sync::Mutex;
use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::{future::BoxFuture, FutureExt};
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use tracing::{event, Level};

use crate::{id::GatewayAddress, networking::ConnectionManager};

pub struct Subscription(Option<Box<dyn (FnOnce() -> ()) + Send>>);

impl Drop for Subscription {
  fn drop(&mut self) {
    match self.0.take() {
      None => (),
      Some(f) => f(),
    }
  }
}

/// Interface that provides Aktør gateways information.
#[async_trait]
pub trait GatewayListProvider: Send {
  /// Initializes the provider
  async fn new() -> Self;

  /// Returns the list of gateways (nodes) that can be used by a client to connect to Aktør cluster.
  /// The uri is in the form of a valid ZeroMQ address.
  async fn get_gateways(&self) -> Arc<[GatewayAddress]>;

  /// Specifies how often this GatewayListProvider is refreshed, to have a bound on max staleness of its returned information.
  fn max_staleness(&self) -> Duration;
}

pub enum SubscriptionStatus {
  Live,
  Dead,
}

/// An optional interface that GatewayListProvider may implement if it support out of band gw update notifications.
/// By default GatewayListProvider should support pull based queries (get_gateways).
/// Optionally, some GatewayListProviders may be able to notify a listener if an updated gw information is available.
/// This is optional and not required.
pub trait GatewayListObservable: GatewayListProvider {
  /// Subscribe to get updates whenever the available nodes change.
  fn subscribe_to_gateway_notification_events(
    &self,
    listener: Box<dyn Fn(Arc<[GatewayAddress]>) -> BoxFuture<'static, SubscriptionStatus>>,
  ) -> Option<Subscription>;
}

impl<G: GatewayListProvider> GatewayListObservable for G {
  default fn subscribe_to_gateway_notification_events(
    &self,
    _: Box<dyn Fn(Arc<[GatewayAddress]>) -> BoxFuture<'static, SubscriptionStatus>>,
  ) -> Option<Subscription> {
    None
  }
}

pub struct NullGatewayListProvider;

#[async_trait]
impl GatewayListProvider for NullGatewayListProvider {
  #[inline]
  async fn new() -> Self {
    Self
  }

  #[inline]
  async fn get_gateways(&self) -> Arc<[GatewayAddress]> {
    Arc::from([])
  }

  #[inline]
  fn max_staleness(&self) -> Duration {
    Duration::max_value()
  }
}

struct GatewayManagerInner<P: GatewayListProvider> {
  known_dead: HashMap<GatewayAddress, DateTime<Utc>>,
  known_masked: HashMap<GatewayAddress, DateTime<Utc>>,
  known_gateways: Arc<[GatewayAddress]>,
  cached_live_gateways: Arc<[GatewayAddress]>,
  cached_live_gateways_set: HashSet<GatewayAddress>,
  options: GatewayOptions,
  provider: P,
  provider_subscription: Option<Subscription>,
  round_robin_counter: usize,
  last_refresh_time: DateTime<Utc>,
  rand: SmallRng,
  connection_manager: ConnectionManager,
}

impl<P: GatewayListProvider> GatewayManagerInner<P> {
  // This function is called asynchronously from gateway refresh timer.
  async fn update_live_gateways_snapshot(
    &mut self,
    refreshed_gateways: Arc<[GatewayAddress]>,
    max_staleness: Duration,
  ) {
    // take whatever listProvider gave us and exclude those we think are dead.
    let mut live = Vec::new();
    let now = Utc::now();

    self.known_gateways = refreshed_gateways.clone();
    for trial in self.known_gateways.as_ref() {
      // We consider a node to be dead if we recorded it is dead due to socket error
      // and it was recorded (diedAt) not too long ago (less than maxStaleness ago).
      //
      // The latter is to cover the case when the Gateway provider returns an outdated
      // list that does not yet reflect the actually recently died Gateway.
      //
      // If it has passed more than maxStaleness - we assume maxStaleness is the upper
      // bound on Gateway provider freshness.
      let mut is_dead = false;

      if let Some(died_at) = self.known_dead.get(trial) {
        if now - *died_at < max_staleness {
          is_dead = true;
        } else {
          // Remove stale entries.
          self.known_dead.remove(trial);
        }
      }

      if let Some(masked_at) = self.known_masked.get(trial) {
        if now - *masked_at < max_staleness {
          is_dead = true;
        } else {
          // Remove stale entries.
          self.known_masked.remove(trial);
        }
      }

      if !is_dead {
        live.push(trial.clone());
      }
    }

    if live.is_empty() {
      event!(Level::WARN, "All gateways have previously been marked as dead. Clearing the list of dead gateways to expedite reconnection.");
      live.extend(self.known_gateways.iter().cloned());
      self.known_gateways = Arc::from([]);
    }

    self.cached_live_gateways = Arc::from(live.as_ref());
    self.cached_live_gateways_set = self.cached_live_gateways.iter().cloned().collect();

    let prev_refresh = self.last_refresh_time;
    self.last_refresh_time = now;
    event!(
      Level::INFO,
      // known_gateways = self.known_gateways,
      // live_gateways = self.cached_live_gateways,
      // prev_refresh,
      "Refreshed the live Gateway list. Found {} gateways from Gateway list provider: {:?}. Picked only known live out of them. Now has {} live Gateways: {:?}. Previous refresh time was = {}",
      self.known_gateways.len(),
      self.known_gateways,
      self.cached_live_gateways.len(),
      self.cached_live_gateways,
      prev_refresh,
    );

    // Close connections to known dead connections, but keep the "masked" ones.
    // Client will not send any new request to the "masked" connections, but might still
    // receive responses
    let mut connections_to_keep_alive = live;
    connections_to_keep_alive.extend(self.known_masked.iter().map(|(k, _)| k.clone()));
    self
      .close_evicted_gateway_connections(&connections_to_keep_alive)
      .await;
  }

  async fn close_evicted_gateway_connections(&mut self, live_gateways: &[GatewayAddress]) {
    let connected_gateways = self.connection_manager.get_connected_addresses().await;
    for address in connected_gateways {
      let is_live_gateway = live_gateways.iter().any(|live| live.matches(&address));

      if !is_live_gateway {
        event!(
          Level::INFO,
          // endpoint = address,
          "Closing connection to {} because it has been marked as dead",
          address
        );
        self.connection_manager.close(&address).await
      }
    }
  }
}

pub struct GatewayOptions {
  gateway_list_refresh_period: Duration,
  prefered_gateway_index: Option<usize>,
}

impl Default for GatewayOptions {
  #[inline]
  fn default() -> Self {
    Self {
      gateway_list_refresh_period: Duration::minutes(1),
      prefered_gateway_index: None,
    }
  }
}

/// A GatewayManager holds the list of known gateways, as well as maintaining the list of "dead" gateways.
///
/// The known list can come from one of two places: the full list may appear in the client configuration object, or
/// the config object may contain an IGatewayListProvider delegate. If both appear, then the delegate takes priority.
#[derive(Clone)]
pub(crate) struct GatewayManager<P: GatewayListObservable>(Arc<Mutex<GatewayManagerInner<P>>>);

impl<P: GatewayListObservable + 'static> GatewayManager<P> {
  pub async fn new(options: GatewayOptions, connection_manager: ConnectionManager) -> Self {
    let provider = P::new().await;
    let known_gateways = provider.get_gateways().await;
    if known_gateways.is_empty() {
      // TODO: Return result somehow
      panic!("Could not find any gateways.");
    }

    let inner = Arc::new(Mutex::new(GatewayManagerInner {
      known_dead: Default::default(),
      known_masked: Default::default(),
      known_gateways: Arc::from([]),
      cached_live_gateways: Arc::from([]),
      cached_live_gateways_set: Default::default(),
      options,
      provider,
      provider_subscription: None,
      round_robin_counter: 0,
      last_refresh_time: Utc::now(),
      rand: SmallRng::from_entropy(),
      connection_manager,
    }));

    let mgr = GatewayManager(inner.clone());

    let handler = {
      let inner = Arc::downgrade(&inner);

      move |gateways| match inner.upgrade() {
        None => async { SubscriptionStatus::Dead }.boxed(),
        Some(arc) => async move {
          let mut lock = arc.lock_arc().await;
          let staleness = lock.provider.max_staleness();
          lock
            .update_live_gateways_snapshot(gateways, staleness)
            .await;
          SubscriptionStatus::Live
        }
        .boxed(),
      }
    };

    let mut lock = inner.lock_arc().await;
    let inner: &mut GatewayManagerInner<P> = &mut lock;
    let options = &inner.options;
    let subscription = inner
      .provider
      .subscribe_to_gateway_notification_events(Box::new(handler));

    inner.provider_subscription = subscription;
    inner.round_robin_counter = match options.prefered_gateway_index {
      Some(n) => n,
      _ => inner.rand.gen_range(0, known_gateways.len()),
    };
    inner.known_gateways = known_gateways.clone();
    inner.cached_live_gateways = known_gateways.clone();
    inner.cached_live_gateways_set = known_gateways.iter().cloned().collect();
    inner.last_refresh_time = Utc::now();

    mgr
  }
}
