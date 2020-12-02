use std::{
  any::Any,
  borrow::Borrow,
  collections::{
    hash_map::{self, RandomState},
    HashMap,
  },
  fmt,
  fmt::Debug,
  hash::BuildHasher,
  hash::Hash,
  iter::FusedIterator,
  marker::PhantomData,
  ops::Index,
};
#[cfg(debug_assertions)]
use std::{any::TypeId, iter::FromIterator};

trait ValueCaster {
  unsafe fn cast_to_any(&self, b: Box<dyn Value>) -> Box<dyn Any>;
}

struct BoxCaster<V: Value>(PhantomData<V>);

impl<V: Value> BoxCaster<V> {
  const INSTANCE: BoxCaster<V> = BoxCaster(PhantomData);
}

impl<V: Value> ValueCaster for BoxCaster<V> {
  #[inline]
  unsafe fn cast_to_any(&self, b: Box<dyn Value>) -> Box<dyn Any> {
    debug_assert_eq!(Value::type_id(&b), TypeId::of::<V>());

    let raw: *mut dyn Value = Box::into_raw(b);
    let raw = raw as *mut V;
    let raw = raw as *mut dyn Any;
    Box::from_raw(raw)
  }
}

trait Value: Any + Debug {
  fn as_any(&self) -> &dyn Any;
  fn as_mut_any(&mut self) -> &mut dyn Any;
  fn as_debug(&self) -> &dyn Debug;
  fn dyn_caster(&self) -> &'static dyn ValueCaster;

  #[cfg(debug_assertions)]
  fn type_id(&self) -> TypeId;
}

impl<A: Any + Debug> Value for A {
  #[inline]
  fn as_any(&self) -> &dyn Any {
    self as &dyn Any
  }

  #[inline]
  fn as_mut_any(&mut self) -> &mut dyn Any {
    self as &mut dyn Any
  }

  #[inline]
  fn as_debug(&self) -> &dyn Debug {
    self as &dyn Debug
  }

  fn dyn_caster(&self) -> &'static dyn ValueCaster {
    &BoxCaster::<A>::INSTANCE
  }

  #[cfg(debug_assertions)]
  #[inline]
  fn type_id(&self) -> TypeId {
    TypeId::of::<Self>()
  }
}

#[inline]
fn cast_to_any(b: Box<dyn Value>) -> Box<dyn Any> {
  let caster = b.dyn_caster();

  // SAFETY: The caster we get here is given by a blanket implementation
  // for Value - and is the only way to implement Value (it's a private
  // trait). So it should allways expect the correct boxed type.
  unsafe { caster.cast_to_any(b) }
}

#[inline]
fn new_value(v: impl Any + Debug) -> Box<dyn Value> {
  Box::new(v)
}

pub struct AnyMap<K, S = RandomState> {
  map: HashMap<K, Box<dyn Value>, S>,
}

impl<K> AnyMap<K, RandomState> {
  /// Creates an empty `AnyMap`.
  ///
  /// The any map is initially created with a capacity of 0, so it will not allocate until it
  /// is first inserted into.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// ```
  #[inline]
  pub fn new() -> AnyMap<K, RandomState> {
    Default::default()
  }

  /// Creates an empty `AnyMap` with the specified capacity.
  ///
  /// The any map will be able to hold at least `capacity` elements without
  /// reallocating. If `capacity` is 0, the hash map will not allocate.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::with_capacity(10);
  /// ```
  #[inline]
  pub fn with_capacity(capacity: usize) -> AnyMap<K, RandomState> {
    AnyMap::with_capacity_and_hasher(capacity, Default::default())
  }
}

impl<K, S> AnyMap<K, S> {
  /// Creates an empty `AnyMap` which will use the given hash builder to hash
  /// keys.
  ///
  /// The created map has the default initial capacity.
  ///
  /// Warning: `hash_builder` is normally randomly generated, and
  /// is designed to allow AnyMaps to be resistant to attacks that
  /// cause many collisions and very poor performance. Setting it
  /// manually using this function can expose a DoS attack vector.
  ///
  /// The `hash_builder` passed should implement the [`BuildHasher`] trait for
  /// the HashMap to be useful, see its documentation for details.
  ///
  /// # Examples
  ///
  /// ```
  /// use std::collections::hash_map::RandomState;
  ///
  /// let s = RandomState::new();
  /// let mut map = AnyMap::with_hasher(s);
  /// map.insert(1, 2);
  /// ```
  #[inline]
  pub fn with_hasher(hash_builder: S) -> AnyMap<K, S> {
    AnyMap {
      map: HashMap::with_hasher(hash_builder),
    }
  }

  /// Creates an empty `HashMap` with the specified capacity, using `hash_builder`
  /// to hash the keys.
  ///
  /// The hash map will be able to hold at least `capacity` elements without
  /// reallocating. If `capacity` is 0, the hash map will not allocate.
  ///
  /// Warning: `hash_builder` is normally randomly generated, and
  /// is designed to allow HashMaps to be resistant to attacks that
  /// cause many collisions and very poor performance. Setting it
  /// manually using this function can expose a DoS attack vector.
  ///
  /// The `hash_builder` passed should implement the [`BuildHasher`] trait for
  /// the HashMap to be useful, see its documentation for details.
  ///
  /// # Examples
  ///
  /// ```
  /// use std::collections::hash_map::RandomState;
  ///
  /// let s = RandomState::new();
  /// let mut map = AnyMap::with_capacity_and_hasher(10, s);
  /// map.insert(1, 2);
  /// ```
  #[inline]
  pub fn with_capacity_and_hasher(capacity: usize, hash_builder: S) -> AnyMap<K, S> {
    AnyMap {
      map: HashMap::with_capacity_and_hasher(capacity, hash_builder),
    }
  }

  /// Returns the number of elements the map can hold without reallocating.
  ///
  /// This number is a lower bound; the `HashMap<K, V>` might be able to hold
  /// more, but is guaranteed to be able to hold at least this many.
  ///
  /// # Examples
  ///
  /// ```
  /// let map: AnyMap<i32> = HashMap::with_capacity(100);
  /// assert!(map.capacity() >= 100);
  /// ```
  #[inline]
  pub fn capacity(&self) -> usize {
    self.map.capacity()
  }

  /// An iterator visiting all keys in arbitrary order.
  /// The iterator element type is `&'a K`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// for key in map.keys() {
  ///     println!("{}", key);
  /// }
  /// ```
  pub fn keys(&self) -> Keys<'_, K> {
    Keys {
      inner: self.iter_dyn(),
    }
  }

  /// An iterator visiting all values in arbitrary order.
  /// The iterator element type is `&'a V`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  ///
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// for val in map.values_any() {
  ///     println!("{}", val);
  /// }
  /// ```
  pub fn values_dyn(&self) -> ValuesDyn<'_, K> {
    ValuesDyn {
      inner: self.iter_dyn(),
    }
  }

  /// An iterator visiting all values mutably in arbitrary order.
  /// The iterator element type is `&'a mut V`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  ///
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// for val in map.values_mut_any() {
  ///     *val = *val + 10;
  /// }
  ///
  /// for val in map.values_any() {
  ///     println!("{}", val);
  /// }
  /// ```
  pub fn values_mut_dyn(&mut self) -> ValuesMutDyn<'_, K> {
    ValuesMutDyn {
      inner: self.iter_mut_dyn(),
    }
  }

  /// An iterator visiting all key-value pairs in arbitrary order.
  /// The iterator element type is `(&'a K, &'a V)`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// for (key, val) in map.iter_any() {
  ///     println!("key: {} val: {}", key, val);
  /// }
  /// ```
  pub fn iter_dyn(&self) -> IterDyn<'_, K> {
    IterDyn {
      base: self.map.iter(),
    }
  }

  /// An iterator visiting all key-value pairs in arbitrary order,
  /// with mutable references to the values.
  /// The iterator element type is `(&'a K, &'a mut V)`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// // Update all values
  /// for (_, val) in map.iter_mut_any() {
  ///     *val *= 2;
  /// }
  ///
  /// for (key, val) in &map {
  ///     println!("key: {} val: {}", key, val);
  /// }
  /// ```
  pub fn iter_mut_dyn(&mut self) -> IterMutDyn<'_, K> {
    IterMutDyn {
      base: self.map.iter_mut(),
    }
  }

  /// Returns the number of elements in the map.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut a = AnyMap::new();
  /// assert_eq!(a.len(), 0);
  /// a.insert(1, "a");
  /// assert_eq!(a.len(), 1);
  /// ```
  #[inline]
  pub fn len(&self) -> usize {
    self.map.len()
  }

  /// Returns `true` if the map contains no elements.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut a = AnyMap::new();
  /// assert!(a.is_empty());
  /// a.insert(1, "a");
  /// assert!(!a.is_empty());
  /// ```
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.map.is_empty()
  }

  /// Clears the map, returning all key-value pairs as an iterator. Keeps the
  /// allocated memory for reuse.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut a = AnyMap::new();
  /// a.insert(1, "a");
  /// a.insert(2, "b");
  ///
  /// for (k, v) in a.drain().take(1) {
  ///     assert!(k == 1 || k == 2);
  ///     assert!(v == "a" || v == "b");
  /// }
  ///
  /// assert!(a.is_empty());
  /// ```
  #[inline]
  pub fn drain_dyn(&mut self) -> DrainDyn<'_, K> {
    DrainDyn {
      base: self.map.drain(),
    }
  }

  /// Clears the map, removing all key-value pairs. Keeps the allocated memory
  /// for reuse.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut a = AnyMap::new();
  /// a.insert(1, "a");
  /// a.clear();
  /// assert!(a.is_empty());
  /// ```
  #[inline]
  pub fn clear(&mut self) {
    self.map.clear();
  }

  /// Returns a reference to the map's [`BuildHasher`].
  ///
  /// # Examples
  ///
  /// ```
  /// use std::collections::hash_map::RandomState;
  ///
  /// let hasher = RandomState::new();
  /// let map: AnyMap<i32> = AnyMap::with_hasher(hasher);
  /// let hasher: &RandomState = map.hasher();
  /// ```
  #[inline]
  pub fn hasher(&self) -> &S {
    self.map.hasher()
  }
}

impl<K, S> AnyMap<K, S>
where
  K: Eq + Hash,
  S: BuildHasher,
{
  /// Reserves capacity for at least `additional` more elements to be inserted
  /// in the `AnyMap`. The collection may reserve more space to avoid
  /// frequent reallocations.
  ///
  /// # Panics
  ///
  /// Panics if the new allocation size overflows [`usize`].
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.reserve(10);
  /// ```
  #[inline]
  pub fn reserve(&mut self, additional: usize) {
    self.map.reserve(additional)
  }

  /// Shrinks the capacity of the map as much as possible. It will drop
  /// down as much as possible while maintaining the internal rules
  /// and possibly leaving some space in accordance with the resize policy.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<i32, i32> = AnyMap::with_capacity(100);
  /// map.insert(1, 2);
  /// map.insert(3, 4);
  /// assert!(map.capacity() >= 100);
  /// map.shrink_to_fit();
  /// assert!(map.capacity() >= 2);
  /// ```
  #[inline]
  pub fn shrink_to_fit(&mut self) {
    self.map.shrink_to_fit();
  }

  /// Gets the given key's corresponding entry in the map for in-place manipulation.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut letters = AnyMap::new();
  ///
  /// for ch in "a short treatise on fungi".chars() {
  ///     let counter = letters.entry(ch).or_insert(0);
  ///     *counter += 1;
  /// }
  ///
  /// assert_eq!(letters[&'s'], 2);
  /// assert_eq!(letters[&'t'], 3);
  /// assert_eq!(letters[&'u'], 1);
  /// assert_eq!(letters.get(&'y'), None);
  /// ```
  #[inline]
  pub fn entry_dyn(&mut self, key: K) -> EntryDyn<'_, K> {
    self.map.entry(key).into()
  }

  /// Returns a reference to the value corresponding to the key.
  ///
  /// The key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// assert_eq!(map.get(&1), Some(&"a"));
  /// assert_eq!(map.get(&2), None);
  /// ```
  #[inline]
  pub fn get_dyn<Q: ?Sized>(&self, k: &Q) -> Option<&dyn Any>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.get(k).map(Value::as_any)
  }

  /// Returns the key-value pair corresponding to the supplied key.
  ///
  /// The supplied key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// assert_eq!(map.get_key_value(&1), Some((&1, &"a")));
  /// assert_eq!(map.get_key_value(&2), None);
  /// ```
  #[inline]
  pub fn get_key_value_dyn<Q: ?Sized>(&self, k: &Q) -> Option<(&K, &dyn Any)>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.get_key_value(k).map(|(k, v)| (k, v.as_any()))
  }

  /// Returns `true` if the map contains a value for the specified key.
  ///
  /// The key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// assert_eq!(map.contains_key(&1), true);
  /// assert_eq!(map.contains_key(&2), false);
  /// ```
  #[inline]
  pub fn contains_key<Q: ?Sized>(&self, k: &Q) -> bool
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.contains_key(k)
  }

  /// Returns a mutable reference to the value corresponding to the key.
  ///
  /// The key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// if let Some(x) = map.get_mut(&1) {
  ///     *x = "b";
  /// }
  /// assert_eq!(map[&1], "b");
  /// ```
  #[inline]
  pub fn get_mut_dyn<Q: ?Sized>(&mut self, k: &Q) -> Option<&mut dyn Any>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.get_mut(k).map(Value::as_mut_any)
  }

  /// Inserts a key-value pair into the map.
  ///
  /// If the map did not have this key present, [`None`] is returned.
  ///
  /// If the map did have this key present, the value is updated, and the old
  /// value is returned. The key is not updated, though; this matters for
  /// types that can be `==` without being identical. See the [module-level
  /// documentation] for more.
  ///
  /// [module-level documentation]: crate::collections#insert-and-complex-keys
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// assert_eq!(map.insert(37, "a"), None);
  /// assert_eq!(map.is_empty(), false);
  ///
  /// map.insert(37, "b");
  /// assert_eq!(map.insert(37, "c"), Some("b"));
  /// assert_eq!(map[&37], "c");
  /// ```
  #[inline]
  pub fn insert(&mut self, k: K, v: impl Any + Debug) -> Option<Box<dyn Any>> {
    self.map.insert(k, Box::new(v)).map(cast_to_any)
  }

  /// Removes a key from the map, returning the value at the key if the key
  /// was previously in the map.
  ///
  /// The key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// assert_eq!(map.remove(&1), Some("a"));
  /// assert_eq!(map.remove(&1), None);
  /// ```
  #[inline]
  pub fn remove<Q: ?Sized>(&mut self, k: &Q) -> Option<Box<dyn Any>>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.remove(k).map(cast_to_any)
  }

  /// Removes a key from the map, returning the stored key and value if the
  /// key was previously in the map.
  ///
  /// The key may be any borrowed form of the map's key type, but
  /// [`Hash`] and [`Eq`] on the borrowed form *must* match those for
  /// the key type.
  ///
  /// # Examples
  ///
  /// ```
  /// # fn main() {
  /// let mut map = AnyMap::new();
  /// map.insert(1, "a");
  /// assert_eq!(map.remove_entry(&1), Some((1, "a")));
  /// assert_eq!(map.remove(&1), None);
  /// # }
  /// ```
  #[inline]
  pub fn remove_entry<Q: ?Sized>(&mut self, k: &Q) -> Option<(K, Box<dyn Any>)>
  where
    K: Borrow<Q>,
    Q: Hash + Eq,
  {
    self.map.remove_entry(k).map(|(k, v)| (k, cast_to_any(v)))
  }

  /// Retains only the elements specified by the predicate.
  ///
  /// In other words, remove all pairs `(k, v)` such that `f(&k,&mut v)` returns `false`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<i32> = (0..8).map(|x|(x, x*10)).collect();
  /// map.retain(|&k, _| k % 2 == 0);
  /// assert_eq!(map.len(), 4);
  /// ```
  #[inline]
  pub fn retain_dyn<F>(&mut self, mut f: F)
  where
    F: FnMut(&K, &mut dyn Any) -> bool,
  {
    self.map.retain(|k, v| f(k, v.as_mut_any()))
  }
}

impl<K, S> Default for AnyMap<K, S>
where
  S: Default,
{
  /// Creates an empty `AnyMap<K, V, S>`, with the `Default` value for the hasher.
  #[inline]
  fn default() -> AnyMap<K, S> {
    AnyMap::with_hasher(Default::default())
  }
}

impl<K, Q: ?Sized, S> Index<&Q> for AnyMap<K, S>
where
  K: Eq + Hash + Borrow<Q>,
  Q: Eq + Hash,
  S: BuildHasher,
{
  type Output = dyn Any;

  /// Returns a reference to the value corresponding to the supplied key.
  ///
  /// # Panics
  ///
  /// Panics if the key is not present in the `HashMap`.
  #[inline]
  fn index(&self, key: &Q) -> &dyn Any {
    self.get_dyn(key).expect("no entry found for key")
  }
}

/// An iterator over the dynamic entries of a `AnyMap`.
///
/// This `struct` is created by the [`iter_dyn`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`iter`]: AnyMap::iter_dyn
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter = map.iter_dyn();
/// ```
pub struct IterDyn<'a, K: 'a> {
  base: hash_map::Iter<'a, K, Box<dyn Value>>,
}

impl<K> Clone for IterDyn<'_, K> {
  #[inline]
  fn clone(&self) -> Self {
    IterDyn {
      base: self.base.clone(),
    }
  }
}

/// A mutable iterator over the entries of a `AnyMap`.
///
/// This `struct` is created by the [`iter_mut_dyn`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`iter_mut`]: HashMap::iter_mut_dyn
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter = map.iter_mut_dyn();
/// ```
pub struct IterMutDyn<'a, K: 'a> {
  base: hash_map::IterMut<'a, K, Box<dyn Value>>,
}

/// An owning iterator over the entries of a `AnyMap`.
///
/// This `struct` is created by the [`into_iter`] method on [`AnyMap`]
/// (provided by the `IntoIterator` trait). See its documentation for more.
///
/// [`into_iter`]: IntoIterator::into_iter
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter = map.into_iter();
/// ```
pub struct IntoIter<K> {
  base: hash_map::IntoIter<K, Box<dyn Value>>,
}

/// An iterator over the keys of a `AnyMap`.
///
/// This `struct` is created by the [`keys`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`keys`]: AnyMap::keys
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter_keys = map.keys();
/// ```
pub struct Keys<'a, K: 'a> {
  inner: IterDyn<'a, K>,
}

impl<K> Clone for Keys<'_, K> {
  #[inline]
  fn clone(&self) -> Self {
    Keys {
      inner: self.inner.clone(),
    }
  }
}

impl<K: Debug> fmt::Debug for Keys<'_, K> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_list().entries(self.clone()).finish()
  }
}

/// An iterator over the values of a `AnyMap`.
///
/// This `struct` is created by the [`values_dyn`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`values`]: AnyMap::values_dyn
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter_values = map.values();
/// ```
pub struct ValuesDyn<'a, K: 'a> {
  inner: IterDyn<'a, K>,
}

// FIXME(#26925) Remove in favor of `#[derive(Clone)]`
impl<K> Clone for ValuesDyn<'_, K> {
  #[inline]
  fn clone(&self) -> Self {
    ValuesDyn {
      inner: self.inner.clone(),
    }
  }
}

/// A draining iterator over the entries of a `AnyMap`.
///
/// This `struct` is created by the [`drain_dyn`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`drain`]: AnyMap::drain
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter = map.drain();
/// ```
pub struct DrainDyn<'a, K: 'a> {
  base: hash_map::Drain<'a, K, Box<dyn Value>>,
}

/// A mutable iterator over the values of a `AnyMap`.
///
/// This `struct` is created by the [`values_mut_dyn`] method on [`AnyMap`]. See its
/// documentation for more.
///
/// [`values_mut_dyn`]: AnyMap::values_mut_dyn
///
/// # Example
///
/// ```
/// let mut map = AnyMap::new();
/// map.insert("a", 1);
/// let iter_values = map.values_mut();
/// ```
pub struct ValuesMutDyn<'a, K: 'a> {
  inner: IterMutDyn<'a, K>,
}

/// A view into a single entry in a map, which may either be vacant or occupied.
///
/// This `enum` is constructed from the [`entry`] method on [`AnyMap`].
///
/// [`entry`]: AnyMap::entry
pub enum EntryDyn<'a, K: 'a> {
  /// An occupied entry.
  Occupied(OccupiedEntryDyn<'a, K>),
  /// A vacant entry.
  Vacant(VacantEntryDyn<'a, K>),
}

impl<'a, K: 'a> From<hash_map::Entry<'a, K, Box<dyn Value>>> for EntryDyn<'a, K> {
  #[inline]
  fn from(e: hash_map::Entry<'a, K, Box<dyn Value>>) -> Self {
    match e {
      hash_map::Entry::Occupied(e) => Self::Occupied(e.into()),
      hash_map::Entry::Vacant(e) => Self::Vacant(e.into()),
    }
  }
}

/// A view into an occupied entry in a `AnyMap`.
/// It is part of the [`EntryDyn`] enum.
pub struct OccupiedEntryDyn<'a, K: 'a> {
  base: hash_map::OccupiedEntry<'a, K, Box<dyn Value>>,
}

impl<'a, K: 'a> From<hash_map::OccupiedEntry<'a, K, Box<dyn Value>>> for OccupiedEntryDyn<'a, K> {
  #[inline]
  fn from(e: hash_map::OccupiedEntry<'a, K, Box<dyn Value>>) -> Self {
    Self { base: e }
  }
}

/// A view into a vacant entry in a `AnyMap`.
/// It is part of the [`Entry`] enum.
pub struct VacantEntryDyn<'a, K: 'a> {
  base: hash_map::VacantEntry<'a, K, Box<dyn Value>>,
}

impl<'a, K: 'a> From<hash_map::VacantEntry<'a, K, Box<dyn Value>>> for VacantEntryDyn<'a, K> {
  #[inline]
  fn from(e: hash_map::VacantEntry<'a, K, Box<dyn Value>>) -> Self {
    Self { base: e }
  }
}

impl<'a, K, S> IntoIterator for &'a AnyMap<K, S> {
  type Item = (&'a K, &'a dyn Any);
  type IntoIter = IterDyn<'a, K>;

  #[inline]
  fn into_iter(self) -> IterDyn<'a, K> {
    self.iter_dyn()
  }
}

impl<'a, K, S> IntoIterator for &'a mut AnyMap<K, S> {
  type Item = (&'a K, &'a mut dyn Any);
  type IntoIter = IterMutDyn<'a, K>;

  #[inline]
  fn into_iter(self) -> IterMutDyn<'a, K> {
    self.iter_mut_dyn()
  }
}

impl<K, S> IntoIterator for AnyMap<K, S> {
  type Item = (K, Box<dyn Any>);
  type IntoIter = IntoIter<K>;

  /// Creates a consuming iterator, that is, one that moves each key-value
  /// pair out of the map in arbitrary order. The map cannot be used after
  /// calling this.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map = AnyMap::new();
  /// map.insert("a", 1);
  /// map.insert("b", 2);
  /// map.insert("c", 3);
  ///
  /// // Not possible with .iter()
  /// let vec: Vec<(&str, Box<dyn Any>)> = map.into_iter().collect();
  /// ```
  #[inline]
  fn into_iter(self) -> IntoIter<K> {
    IntoIter {
      base: self.map.into_iter(),
    }
  }
}

impl<'a, K> Iterator for IterDyn<'a, K> {
  type Item = (&'a K, &'a dyn Any);

  #[inline]
  fn next(&mut self) -> Option<(&'a K, &'a dyn Any)> {
    self.base.next().map(|(k, v)| (k, v.as_any()))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.base.size_hint()
  }
}

impl<K> ExactSizeIterator for IterDyn<'_, K> {
  #[inline]
  fn len(&self) -> usize {
    self.base.len()
  }
}

impl<K> FusedIterator for IterDyn<'_, K> {}

impl<'a, K> Iterator for IterMutDyn<'a, K> {
  type Item = (&'a K, &'a mut dyn Any);

  #[inline]
  fn next(&mut self) -> Option<(&'a K, &'a mut dyn Any)> {
    self.base.next().map(|(k, v)| (k, v.as_mut_any()))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.base.size_hint()
  }
}

impl<K> ExactSizeIterator for IterMutDyn<'_, K> {
  #[inline]
  fn len(&self) -> usize {
    self.base.len()
  }
}

impl<K> FusedIterator for IterMutDyn<'_, K> {}

impl<K> Iterator for IntoIter<K> {
  type Item = (K, Box<dyn Any>);

  #[inline]
  fn next(&mut self) -> Option<(K, Box<dyn Any>)> {
    self.base.next().map(|(k, v)| (k, cast_to_any(v)))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.base.size_hint()
  }
}

impl<K> ExactSizeIterator for IntoIter<K> {
  #[inline]
  fn len(&self) -> usize {
    self.base.len()
  }
}

impl<K> FusedIterator for IntoIter<K> {}

impl<'a, K> Iterator for Keys<'a, K> {
  type Item = &'a K;

  #[inline]
  fn next(&mut self) -> Option<&'a K> {
    self.inner.next().map(|(k, _)| k)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.inner.size_hint()
  }
}

impl<K> ExactSizeIterator for Keys<'_, K> {
  #[inline]
  fn len(&self) -> usize {
    self.inner.len()
  }
}

impl<K> FusedIterator for Keys<'_, K> {}

impl<'a, K> Iterator for ValuesDyn<'a, K> {
  type Item = &'a dyn Any;

  #[inline]
  fn next(&mut self) -> Option<&'a dyn Any> {
    self.inner.next().map(|(_, v)| v)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.inner.size_hint()
  }
}

impl<K> ExactSizeIterator for ValuesDyn<'_, K> {
  #[inline]
  fn len(&self) -> usize {
    self.inner.len()
  }
}

impl<K> FusedIterator for ValuesDyn<'_, K> {}

impl<'a, K> Iterator for ValuesMutDyn<'a, K> {
  type Item = &'a mut dyn Any;

  #[inline]
  fn next(&mut self) -> Option<&'a mut dyn Any> {
    self.inner.next().map(|(_, v)| v)
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.inner.size_hint()
  }
}

impl<K> ExactSizeIterator for ValuesMutDyn<'_, K> {
  #[inline]
  fn len(&self) -> usize {
    self.inner.len()
  }
}

impl<K> FusedIterator for ValuesMutDyn<'_, K> {}

impl<'a, K> Iterator for DrainDyn<'a, K> {
  type Item = (K, Box<dyn Any>);

  #[inline]
  fn next(&mut self) -> Option<(K, Box<dyn Any>)> {
    self.base.next().map(|(k, v)| (k, cast_to_any(v)))
  }

  #[inline]
  fn size_hint(&self) -> (usize, Option<usize>) {
    self.base.size_hint()
  }
}

impl<'a, K> EntryDyn<'a, K> {
  /// Ensures a value is in the entry by inserting the default if empty, and returns
  /// a mutable reference to the value in the entry.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  ///
  /// map.entry("poneyland").or_insert(3);
  /// assert_eq!(map["poneyland"], 3);
  ///
  /// *map.entry("poneyland").or_insert(10) *= 2;
  /// assert_eq!(map["poneyland"], 6);
  /// ```
  #[inline]
  pub fn or_insert(self, default: impl Any + Debug) -> &'a mut dyn Any {
    match self {
      Self::Occupied(entry) => entry.into_mut(),
      Self::Vacant(entry) => entry.insert(default),
    }
  }

  /// Ensures a value is in the entry by inserting the result of the default function if empty,
  /// and returns a mutable reference to the value in the entry.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// let s = "hoho".to_string();
  ///
  /// map.entry("poneyland").or_insert_with(|| s);
  ///
  /// assert_eq!(map["poneyland"], "hoho".to_string());
  /// ```
  #[inline]
  pub fn or_insert_with<V: Any + Debug, F: FnOnce() -> V>(self, default: F) -> &'a mut dyn Any {
    match self {
      Self::Occupied(entry) => entry.into_mut(),
      Self::Vacant(entry) => entry.insert(default()),
    }
  }

  /// Returns a reference to this entry's key.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
  /// ```
  #[inline]
  pub fn key(&self) -> &K {
    match *self {
      Self::Occupied(ref entry) => entry.key(),
      Self::Vacant(ref entry) => entry.key(),
    }
  }

  /// Provides in-place mutable access to an occupied entry before any
  /// potential inserts into the map.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str, u32> = AnyMap::new();
  ///
  /// map.entry("poneyland")
  ///    .and_modify(|e| { *e += 1 })
  ///    .or_insert(42);
  /// assert_eq!(map["poneyland"], 42);
  ///
  /// map.entry("poneyland")
  ///    .and_modify(|e| { *e += 1 })
  ///    .or_insert(42);
  /// assert_eq!(map["poneyland"], 43);
  /// ```
  #[inline]
  pub fn and_modify<F>(self, f: F) -> Self
  where
    F: FnOnce(&mut dyn Any),
  {
    match self {
      Self::Occupied(mut entry) => {
        f(entry.get_mut());
        Self::Occupied(entry)
      }
      Self::Vacant(entry) => Self::Vacant(entry),
    }
  }
}

impl<'a, K> OccupiedEntryDyn<'a, K> {
  /// Gets a reference to the key in the entry.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
  /// ```
  #[inline]
  pub fn key(&self) -> &K {
    self.base.key()
  }

  /// Take the ownership of the key and value from the map.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// if let Entry::Occupied(o) = map.entry("poneyland") {
  ///     // We delete the entry from the map.
  ///     o.remove_entry();
  /// }
  ///
  /// assert_eq!(map.contains_key("poneyland"), false);
  /// ```
  #[inline]
  pub fn remove_entry(self) -> (K, Box<dyn Any>) {
    let (k, v) = self.base.remove_entry();
    (k, cast_to_any(v))
  }

  /// Gets a reference to the value in the entry.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// if let Entry::Occupied(o) = map.entry("poneyland") {
  ///     assert_eq!(o.get(), &12);
  /// }
  /// ```
  #[inline]
  pub fn get(&self) -> &dyn Any {
    self.base.get().as_any()
  }

  /// Gets a mutable reference to the value in the entry.
  ///
  /// If you need a reference to the `OccupiedEntry` which may outlive the
  /// destruction of the `Entry` value, see [`into_mut`].
  ///
  /// [`into_mut`]: Self::into_mut
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// assert_eq!(map["poneyland"], 12);
  /// if let Entry::Occupied(mut o) = map.entry("poneyland") {
  ///     *o.get_mut() += 10;
  ///     assert_eq!(*o.get(), 22);
  ///
  ///     // We can use the same Entry multiple times.
  ///     *o.get_mut() += 2;
  /// }
  ///
  /// assert_eq!(map["poneyland"], 24);
  /// ```
  #[inline]
  pub fn get_mut(&mut self) -> &mut dyn Any {
    self.base.get_mut().as_mut_any()
  }

  /// Converts the OccupiedEntry into a mutable reference to the value in the entry
  /// with a lifetime bound to the map itself.
  ///
  /// If you need multiple references to the `OccupiedEntry`, see [`get_mut`].
  ///
  /// [`get_mut`]: Self::get_mut
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// assert_eq!(map["poneyland"], 12);
  /// if let Entry::Occupied(o) = map.entry("poneyland") {
  ///     *o.into_mut() += 10;
  /// }
  ///
  /// assert_eq!(map["poneyland"], 22);
  /// ```
  #[inline]
  pub fn into_mut(self) -> &'a mut dyn Any {
    self.base.into_mut().as_mut_any()
  }

  /// Sets the value of the entry, and returns the entry's old value.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// if let Entry::Occupied(mut o) = map.entry("poneyland") {
  ///     assert_eq!(o.insert(15), 12);
  /// }
  ///
  /// assert_eq!(map["poneyland"], 15);
  /// ```
  #[inline]
  pub fn insert(&mut self, value: impl Any + Debug) -> Box<dyn Any> {
    cast_to_any(self.base.insert(Box::new(value)))
  }

  /// Takes the value out of the entry, and returns it.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str, u32> = AnyMap::new();
  /// map.entry("poneyland").or_insert(12);
  ///
  /// if let Entry::Occupied(o) = map.entry("poneyland") {
  ///     assert_eq!(o.remove(), 12);
  /// }
  ///
  /// assert_eq!(map.contains_key("poneyland"), false);
  /// ```
  #[inline]
  pub fn remove(self) -> Box<dyn Any> {
    cast_to_any(self.base.remove())
  }
}

impl<'a, K: 'a> VacantEntryDyn<'a, K> {
  /// Gets a reference to the key that would be used when inserting a value
  /// through the `VacantEntry`.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  /// assert_eq!(map.entry("poneyland").key(), &"poneyland");
  /// ```
  #[inline]
  pub fn key(&self) -> &K {
    self.base.key()
  }

  /// Take ownership of the key.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  ///
  /// if let Entry::Vacant(v) = map.entry("poneyland") {
  ///     v.into_key();
  /// }
  /// ```
  #[inline]
  pub fn into_key(self) -> K {
    self.base.into_key()
  }

  /// Sets the value of the entry with the VacantEntry's key,
  /// and returns a mutable reference to it.
  ///
  /// # Examples
  ///
  /// ```
  /// let mut map: AnyMap<&str> = AnyMap::new();
  ///
  /// if let Entry::Vacant(o) = map.entry("poneyland") {
  ///     o.insert(37);
  /// }
  /// assert_eq!(map["poneyland"], 37);
  /// ```
  #[inline]
  pub fn insert(self, value: impl Any + Debug) -> &'a mut dyn Any {
    self.base.insert(Box::new(value)).as_mut_any()
  }
}

impl<K, V, S> FromIterator<(K, V)> for AnyMap<K, S>
where
  K: Eq + Hash,
  V: Any + Debug,
  S: BuildHasher + Default,
{
  fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> AnyMap<K, S> {
    let mut map = AnyMap::with_hasher(Default::default());
    map.extend(iter);
    map
  }
}

/// Inserts all new key-values from the iterator and replaces values with existing
/// keys with new values returned from the iterator.
impl<K, V, S> Extend<(K, V)> for AnyMap<K, S>
where
  K: Eq + Hash,
  V: Any + Debug,
  S: BuildHasher,
{
  #[inline]
  fn extend<T: IntoIterator<Item = (K, V)>>(&mut self, iter: T) {
    self
      .map
      .extend(iter.into_iter().map(|(k, v)| (k, new_value(v))))
  }
}
