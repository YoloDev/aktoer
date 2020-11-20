use static_assertions::assert_impl_all;

use crate::reactor::ReactorRef;

#[derive(Clone, Debug)]
pub struct Context(ReactorRef);

impl AsRef<ReactorRef> for Context {
  #[inline]
  fn as_ref(&self) -> &ReactorRef {
    &self.0
  }
}

assert_impl_all!(Context: Send, Sync);

impl Context {
  #[inline]
  pub fn new() -> Self {
    Self(ReactorRef::new())
  }
}

impl Default for Context {
  #[inline]
  fn default() -> Self {
    Context::new()
  }
}
