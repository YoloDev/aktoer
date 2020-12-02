use crate::{
  serialization::{FromBytes, ToBytes},
  util::Interner,
};
use chrono::{DateTime, Duration, TimeZone, Utc};
use lazy_static::lazy_static;
use serde::{de, Deserialize, Serialize};
use std::{
  convert::TryFrom,
  convert::TryInto,
  error::Error,
  fmt,
  hash::{Hash, Hasher},
  net::Ipv6Addr,
  net::{IpAddr, Ipv4Addr},
  str::FromStr,
  sync::Arc,
};

trait Ip {
  fn normalize(&mut self);
  fn map_to_ipv4(&self) -> Option<Ipv4Addr>;
  fn map_to_ipv6(&self) -> Ipv6Addr;
}

impl Ip for Ipv4Addr {
  #[inline]
  fn normalize(&mut self) {}

  #[inline]
  fn map_to_ipv4(&self) -> Option<Ipv4Addr> {
    Some(self.clone())
  }

  fn map_to_ipv6(&self) -> Ipv6Addr {
    let [a, b, c, d] = self.octets();
    Ipv6Addr::from([0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d])
  }
}

impl Ip for Ipv6Addr {
  #[inline]
  fn normalize(&mut self) {}

  #[inline]
  fn map_to_ipv4(&self) -> Option<Ipv4Addr> {
    match self.octets() {
      [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xff, 0xff, a, b, c, d] => Some(Ipv4Addr::new(a, b, c, d)),
      _ => None,
    }
  }

  #[inline]
  fn map_to_ipv6(&self) -> Ipv6Addr {
    self.clone()
  }
}

impl Ip for IpAddr {
  fn normalize(&mut self) {
    if let IpAddr::V6(v6) = self {
      if let Some(v4) = v6.map_to_ipv4() {
        *self = IpAddr::V4(v4)
      }
    }
  }

  fn map_to_ipv4(&self) -> Option<Ipv4Addr> {
    match self {
      IpAddr::V4(v4) => v4.map_to_ipv4(),
      IpAddr::V6(v6) => v6.map_to_ipv4(),
    }
  }

  fn map_to_ipv6(&self) -> Ipv6Addr {
    match self {
      IpAddr::V4(v4) => v4.map_to_ipv6(),
      IpAddr::V6(v6) => v6.map_to_ipv6(),
    }
  }
}

#[derive(Eq, PartialEq, Ord, PartialOrd, Hash, Copy, Clone)]
pub struct IpEndPoint {
  address: IpAddr,
  port: u32,
}

impl IpEndPoint {
  #[inline]
  pub const fn new(address: IpAddr, port: u32) -> Self {
    IpEndPoint { address, port }
  }

  #[inline]
  fn normalize(&mut self) {
    self.address.normalize()
  }
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct GatewayAddressData {
  endpoint: IpEndPoint,
  generation: i32,
}

/// Data class encapsulating the details of gateway addresses.
///
/// This type is a wrapper around an Arc, so it's cheap to clone.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct GatewayAddress(Arc<GatewayAddressData>);

impl From<Arc<GatewayAddressData>> for GatewayAddress {
  #[inline]
  fn from(arc: Arc<GatewayAddressData>) -> Self {
    Self(arc)
  }
}

const INTERN_CACHE_INITIAL_SIZE: usize =
  Interner::<GatewayAddressData, GatewayAddress>::SIZE_MEDIUM;
const INTERN_CACHE_CLEANUP_INTERVAL: Option<Duration> = None;

lazy_static! {
  static ref GATEWAY_ADDRESS_INTERNER: Interner<GatewayAddressData, GatewayAddressData> =
    Interner::with_capacity_and_cleanup(INTERN_CACHE_INITIAL_SIZE, INTERN_CACHE_CLEANUP_INTERVAL);
  static ref ZERO: GatewayAddress = {
    let endpoint = IpEndPoint::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0);
    let address = GatewayAddressData {
      endpoint,
      generation: 0,
    };
    GatewayAddress(GATEWAY_ADDRESS_INTERNER.intern(address.clone(), address))
  };
  static ref EPOCH: DateTime<Utc> = Utc.ymd(2020, 1, 1).and_hms(0, 0, 0);
}

impl GatewayAddress {
  const SIZE_BYTES: usize = 24; // 16 for the address (IPv4 | IPv6), 4 for the port, 4 for the generation
  const SEPARATOR: char = '@';

  /// Special constant value to indicate an empty GatewayAddress.
  pub fn zero() -> Self {
    ZERO.clone()
  }

  /// Factory for creating new GatewayAddress with specified IP endpoint address and gateway generation number.
  pub fn new(mut endpoint: IpEndPoint, generation: i32) -> Self {
    endpoint.normalize();

    let address = GatewayAddressData {
      endpoint,
      generation,
    };
    GATEWAY_ADDRESS_INTERNER
      .intern(address.clone(), address)
      .into()
  }

  #[inline]
  pub fn is_client(&self) -> bool {
    self.0.generation < 0
  }

  /// Allocate a new silo generation number.
  pub fn allocate_new_generation() -> i32 {
    let elapsed = Utc::now() - *EPOCH;
    match elapsed.num_microseconds() {
      Some(v) => v as i32,
      None => panic!("Either my math is really of, or you're using this software 200 millenia after it was created. The warranty has expired.")
    }
  }
}

impl fmt::Display for GatewayAddress {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}:{}@{}",
      self.0.endpoint.address, self.0.endpoint.port, self.0.generation
    )
  }
}

impl fmt::Debug for GatewayAddress {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(
      f,
      "{}{}:{}@{}",
      if self.is_client() { "C" } else { "S" },
      self.0.endpoint.address,
      self.0.endpoint.port,
      self.0.generation
    )
  }
}

/// An error which can be returned when parsing an gateway address.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct GatewayAddressParseError;

impl fmt::Display for GatewayAddressParseError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str("Failed to parse gateway address")
  }
}

impl Error for GatewayAddressParseError {}

impl FromStr for GatewayAddress {
  type Err = GatewayAddressParseError;

  fn from_str(addr: &str) -> Result<Self, Self::Err> {
    // This must be the "inverse" of fmt::Display, and must be the same across all gateways in a deployment.
    // Basically, this should never change unless the data content of GatewayAddress changes

    // First is the IPEndpoint; then '@'; then the generation
    let at_sign = addr.find(Self::SEPARATOR).ok_or(GatewayAddressParseError)?;
    let ep_str = &addr[0..at_sign];
    let gen_str = &addr[at_sign + 1..];

    // IPEndpoint is the host, then ':', then the port
    let last_colon = ep_str.rfind(':').ok_or(GatewayAddressParseError)?;
    let host_str = &ep_str[0..last_colon];
    let port_str = &ep_str[last_colon + 1..];

    let host = IpAddr::from_str(host_str).map_err(|_| GatewayAddressParseError)?;
    let port = u32::from_str(port_str).map_err(|_| GatewayAddressParseError)?;
    let gen = i32::from_str(gen_str).map_err(|_| GatewayAddressParseError)?;

    Ok(GatewayAddress::new(IpEndPoint::new(host, port), gen))
  }
}

impl GatewayAddress {
  pub fn consistent_hash(&self) -> u64 {
    let mut hasher = seahash::SeaHasher::default();
    self.hash(&mut hasher);
    hasher.finish()
  }

  #[inline]
  pub fn is_same_logical_gateway(&self, other: &Self) -> bool {
    self.0.endpoint == other.0.endpoint
  }

  #[inline]
  pub fn is_successor_of(&self, other: &Self) -> bool {
    self.is_same_logical_gateway(other)
      && self.0.generation != 0
      && other.0.generation != 0
      && self.0.generation > other.0.generation
  }

  #[inline]
  pub fn is_predecessor_of(&self, other: &Self) -> bool {
    self.is_same_logical_gateway(other)
      && self.0.generation != 0
      && other.0.generation != 0
      && self.0.generation < other.0.generation
  }

  /// Two silo addresses match if they are equal or if everything but generation is equal and
  /// one generation or the other is 0.
  #[inline]
  pub fn matches(&self, other: &Self) -> bool {
    self.is_same_logical_gateway(other)
      && ((self.0.generation == other.0.generation)
        || (self.0.generation == 0)
        || (other.0.generation == 0))
  }
}

impl<'a> Into<[u8; GatewayAddress::SIZE_BYTES]> for &'a GatewayAddress {
  fn into(self) -> [u8; GatewayAddress::SIZE_BYTES] {
    let mut buf = [0u8; GatewayAddress::SIZE_BYTES];
    buf[0..16].copy_from_slice(&self.0.endpoint.address.map_to_ipv6().octets());
    buf[16..20].copy_from_slice(&self.0.endpoint.port.to_be_bytes());
    buf[20..24].copy_from_slice(&self.0.generation.to_be_bytes());
    buf
  }
}

impl<'a> From<&'a [u8; GatewayAddress::SIZE_BYTES]> for GatewayAddress {
  fn from(bytes: &'a [u8; GatewayAddress::SIZE_BYTES]) -> Self {
    let address_bytes: [u8; 16] = bytes[0..16].try_into().unwrap();
    let port_bytes: [u8; 4] = bytes[16..20].try_into().unwrap();
    let generation_bytes: [u8; 4] = bytes[20..24].try_into().unwrap();
    let address = Ipv6Addr::from(address_bytes);
    let port = u32::from_be_bytes(port_bytes);
    let gen = i32::from_be_bytes(generation_bytes);

    GatewayAddress::new(IpEndPoint::new(IpAddr::V6(address), port), gen)
  }
}

impl<'a> TryFrom<&'a [u8]> for GatewayAddress {
  type Error = GatewayAddressParseError;

  #[inline]
  fn try_from(bytes: &'a [u8]) -> Result<Self, Self::Error> {
    if let Ok(bytes) = TryInto::<&[u8; GatewayAddress::SIZE_BYTES]>::try_into(bytes) {
      Ok(bytes.into())
    } else {
      Err(GatewayAddressParseError)
    }
  }
}

impl ToBytes for GatewayAddress {
  type Bytes = [u8; GatewayAddress::SIZE_BYTES];

  #[inline]
  fn to_bytes(&self) -> Self::Bytes {
    self.into()
  }
}

impl FromBytes for GatewayAddress {
  type Error = GatewayAddressParseError;

  #[inline]
  fn from_bytes(bytes: &[u8]) -> Result<Self, Self::Error> {
    bytes.try_into()
  }
}

impl Serialize for GatewayAddress {
  fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
  where
    S: serde::Serializer,
  {
    if serializer.is_human_readable() {
      self.to_string().serialize(serializer)
    } else {
      self.to_bytes().serialize(serializer)
    }
  }
}

struct GatewayAddressVisitor;
impl<'de> de::Visitor<'de> for GatewayAddressVisitor {
  type Value = GatewayAddress;

  fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
    formatter.write_str("a gateway address parseable string produced by calling `to_string` on it, or a byte array of length 24")
  }

  fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
  where
    E: de::Error,
  {
    match GatewayAddress::from_str(value) {
      Ok(v) => Ok(v),
      Err(_) => Err(E::invalid_value(de::Unexpected::Str(value), &self)),
    }
  }

  fn visit_bytes<E>(self, v: &[u8]) -> Result<Self::Value, E>
  where
    E: de::Error,
  {
    match v.try_into() {
      Err(_) => Err(E::invalid_value(de::Unexpected::Bytes(v), &self)),
      Ok(v) => Ok(v),
    }
  }
}

impl<'de> Deserialize<'de> for GatewayAddress {
  fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
  where
    D: serde::Deserializer<'de>,
  {
    deserializer.deserialize_string(GatewayAddressVisitor)
  }
}
