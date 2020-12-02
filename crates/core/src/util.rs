mod anymap;

use std::{
  hash::Hash,
  sync::{Arc, Weak},
};

use async_std::{task, task::JoinHandle};
use chrono::Duration;
use dashmap::DashMap;

pub use anymap::AnyMap;

pub(crate) struct Interner<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Send + Sync + 'static,
{
  cache_cleanup_interval: Option<Duration>,
  cache_cleanup_timer: Option<JoinHandle<()>>,
  cache: Arc<DashMap<K, Weak<T>>>,
}

impl<K, T> Drop for Interner<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Send + Sync + 'static,
{
  fn drop(&mut self) {
    match self.cache_cleanup_timer.take() {
      None => (),
      Some(handle) => {
        task::block_on(handle.cancel());
      }
    }
  }
}

impl<K, T> Interner<K, T>
where
  K: Hash + Eq + Send + Sync + 'static,
  T: Send + Sync + 'static,
{
  pub const SIZE_SMALL: usize = 67;
  pub const SIZE_MEDIUM: usize = 1_117;
  pub const SIZE_LARGE: usize = 143_357;
  pub const SIZE_X_LARGE: usize = 2_293_757;

  pub fn new() -> Self {
    Self::with_capacity(Self::SIZE_SMALL)
  }

  pub fn with_capacity(capacity: usize) -> Self {
    Self::with_capacity_and_cleanup(capacity, None)
  }

  pub fn with_capacity_and_cleanup(mut capacity: usize, cleanup_freq: Option<Duration>) -> Self {
    if capacity == 0 {
      capacity = Self::SIZE_MEDIUM;
    }

    let cache = Arc::new(DashMap::with_capacity(capacity));
    let cache_cleanup_interval = cleanup_freq;
    let cache_cleanup_timer = cache_cleanup_interval.map(|duration| {
      let cache = Arc::downgrade(&cache);
      let duration = duration.clone();
      task::spawn(async move {
        loop {
          match cache.upgrade() {
            None => break,
            Some(cache) => {
              task::sleep(duration.to_std().unwrap()).await;
              Interner::cleanup_callback(&cache);
            }
          }
        }
      })
    });

    Self {
      cache_cleanup_interval,
      cache_cleanup_timer,
      cache,
    }
  }

  /// Find cached copy of object with specified key,
  /// otherwise create new one using the supplied creator-function.
  pub fn get_or_insert(&self, key: K, factory: impl FnOnce() -> T) -> Arc<T> {
    match self.cache.entry(key) {
      dashmap::mapref::entry::Entry::Occupied(mut v) => {
        match v.get().upgrade() {
          Some(arc) => arc,
          None => {
            // value has been collected
            let arc = Arc::new(factory());
            v.insert(Arc::downgrade(&arc));
            arc
          }
        }
      }
      dashmap::mapref::entry::Entry::Vacant(v) => {
        let arc = Arc::new(factory());
        v.insert(Arc::downgrade(&arc));
        arc
      }
    }
  }

  /// Try to find a cached object with the specified key.
  pub fn get(&self, key: &K) -> Option<Arc<T>> {
    self.cache.get(key).and_then(|v| v.upgrade())
  }

  /// Find cached copy of object with specified key, otherwise store the supplied one.
  pub fn intern(&self, key: K, value: T) -> Arc<T> {
    self.get_or_insert(key, || value)
  }

  fn cleanup_callback(cache: &DashMap<K, Weak<T>>) {
    cache.retain(|_, v| v.weak_count() > 0)
  }
}
