pub trait ToBytes {
  type Bytes: AsRef<[u8]>;

  fn to_bytes(&self) -> Self::Bytes;
}

pub trait FromBytes: Sized {
  type Error;

  fn from_bytes(bytes: &[u8]) -> Result<Self, Self::Error>;
}
