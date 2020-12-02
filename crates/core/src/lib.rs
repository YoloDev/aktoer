#![feature(min_specialization)]
#![feature(min_const_generics)]

use std::{any::Any, collections::HashMap, marker::PhantomData, net::IpAddr, sync::Arc};

use async_std::sync::Mutex;
use async_trait::async_trait;
use downcast_rs::{impl_downcast, Downcast};
use linkme::distributed_slice;

pub mod id;
pub mod messaging;
pub mod networking;
mod placement;
pub mod serialization;
mod util;

pub use util::AnyMap;

#[distributed_slice]
pub static ACTOR_TYPES: [ActorRegistration] = [..];

pub struct ActorRegistration {
  type_id: &'static [u8],
  create: fn(ActorId) -> Box<dyn DynActor>,
}

impl ActorRegistration {
  fn create(id: ActorId) -> Arc<Mutex<Box<dyn DynActor>>> {
    let t = ACTOR_TYPES
      .iter()
      .find(|f| f.type_id == id.type_id.as_ref())
      .unwrap_or_else(|| panic!("Actor with type_id '{:?}' not found", id.type_id));

    let create = t.create;
    let boxed = create(id);
    Arc::new(Mutex::from(boxed))
  }
}

#[distributed_slice]
pub static MESSAGE_TYPES: [MessageRegistration] = [..];

pub struct MessageRegistration {
  actor_id: &'static [u8],
  message_id: &'static [u8],
  handler: &'static dyn DynMessageHandler,
}

impl MessageRegistration {
  async fn handle(
    actor: &mut dyn DynActor,
    message: Box<dyn DynRequestMessageType>,
  ) -> Box<dyn DynResponseMessageType> {
    let actor_id = actor.actor_type();
    let message_id = message.message_id();
    let m = MESSAGE_TYPES
      .iter()
      .find(|f| f.actor_id == actor_id && f.message_id == message_id)
      .unwrap_or_else(|| {
        panic!(
          "Message with type_id '{:?}:{:?}' not found",
          actor_id, message_id
        )
      });

    m.handler.invoke(actor, message).await
  }
}

#[async_trait]
pub trait DynMessageHandler: Send + Sync + 'static {
  async fn invoke(
    &self,
    actor: &mut dyn DynActor,
    message: Box<dyn DynRequestMessageType>,
  ) -> Box<dyn DynResponseMessageType>;
}
pub trait ActorIdType: Sized + Clone + Eq {
  fn into_id(self) -> Arc<[u8]>;
}

impl ActorIdType for String {
  #[inline]
  fn into_id(self) -> Arc<[u8]> {
    Arc::from(self.as_ref())
  }
}

impl<'a> ActorIdType for &'a str {
  #[inline]
  fn into_id(self) -> Arc<[u8]> {
    Arc::from(self.as_ref())
  }
}

impl<'a> ActorIdType for &'a [u8] {
  #[inline]
  fn into_id(self) -> Arc<[u8]> {
    Arc::from(self)
  }
}

impl ActorIdType for Arc<[u8]> {
  #[inline]
  fn into_id(self) -> Arc<[u8]> {
    self
  }
}

impl ActorIdType for () {
  #[inline]
  fn into_id(self) -> Arc<[u8]> {
    let empty: &[u8] = &[];
    empty.into_id()
  }
}

pub trait ActorType: Sized + 'static {
  type Id: ActorIdType;

  const TYPE_ID: &'static [u8];
}

pub trait DynRequestMessageType: Downcast + Send + Sync + Any + 'static {
  fn actor_id(&self) -> &'static [u8];
  fn message_id(&self) -> &'static [u8];
}

impl_downcast!(DynRequestMessageType);

pub trait RequestMessageType: DynRequestMessageType + Sized {
  type ActorType: ActorType;
  type ResponseType: ResponseMessageType<RequestType = Self>;

  const MESSAGE_ID: &'static [u8];
}

pub trait DynResponseMessageType: Downcast + Send + Sync + Any + 'static {
  fn actor_id(&self) -> &'static [u8];
  fn message_id(&self) -> &'static [u8];
}

impl_downcast!(DynResponseMessageType);

pub trait ResponseMessageType: DynResponseMessageType + Sized {
  type RequestType: RequestMessageType<ResponseType = Self>;
}

impl<M: RequestMessageType> DynRequestMessageType for M {
  #[inline]
  fn actor_id(&self) -> &'static [u8] {
    M::ActorType::TYPE_ID
  }

  #[inline]
  fn message_id(&self) -> &'static [u8] {
    M::MESSAGE_ID
  }
}

impl<M: ResponseMessageType> DynResponseMessageType for M {
  #[inline]
  fn actor_id(&self) -> &'static [u8] {
    <M::RequestType as RequestMessageType>::ActorType::TYPE_ID
  }

  #[inline]
  fn message_id(&self) -> &'static [u8] {
    <M::RequestType as RequestMessageType>::MESSAGE_ID
  }
}

#[derive(Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ActorId {
  id: Arc<[u8]>,
  type_id: Arc<[u8]>,
}

#[derive(Clone)]
pub struct ActorAddress {
  node: Option<IpAddr>,
  id: ActorId,
}

#[derive(Clone)]
pub struct ActorRef<T: ActorType, S: ActorSystem> {
  id: ActorId,
  system: S,
  phantom: PhantomData<T>,
}

#[async_trait]
pub trait DynActor: Downcast + Send + Sync + Any {
  fn actor_type(&self) -> &'static [u8];
  async fn init(&mut self);
  async fn deinit(&mut self);
}

impl_downcast!(DynActor);

pub trait Actor: DynActor {
  type Type: ActorType;

  fn new(id: ActorId) -> Self;
}

#[async_trait]
impl<A: Actor> DynActor for A {
  #[inline]
  fn actor_type(&self) -> &'static [u8] {
    A::Type::TYPE_ID
  }

  async fn init(&mut self) {
    ActorLifetime::init(self).await
  }

  async fn deinit(&mut self) {
    ActorLifetime::deinit(self).await
  }
}

#[async_trait]
pub trait ActorLifetime: Actor {
  async fn init(&mut self);
  async fn deinit(&mut self);
}

#[async_trait]
impl<A: Actor> ActorLifetime for A {
  default async fn init(&mut self) {}
  default async fn deinit(&mut self) {}
}

pub trait ActorExt: Actor {}

#[derive(Clone)]
struct ActorHolder {
  id: ActorId,
  actor: Arc<Mutex<Box<dyn DynActor>>>,
}

impl ActorHolder {
  fn new(id: ActorId, actor: Arc<Mutex<Box<dyn DynActor>>>) -> Self {
    ActorHolder { id, actor }
  }
}

#[derive(Default)]
struct ActorStore {
  actors: HashMap<ActorId, ActorHolder>,
}

#[derive(Default, Clone)]
struct LocalSystem {
  store: Arc<Mutex<ActorStore>>,
}

impl LocalSystem {
  async fn get_holder(&self, target: ActorId) -> ActorHolder {
    let mut lock = self.store.lock_arc().await;
    let mut is_new = false;
    let holder = lock
      .actors
      .entry(target.clone())
      .or_insert_with(|| {
        is_new = true;
        let actor = ActorRegistration::create(target.clone());
        ActorHolder::new(target, actor)
      })
      .clone();

    if is_new {
      let mut actor_lock = holder.actor.lock_arc().await;
      drop(lock);
      actor_lock.init().await;
    } else {
      drop(lock);
    }

    holder
  }
}

#[async_trait]
pub trait ActorSystem: Clone + Send + Sync + 'static {
  fn proxy<T: ActorType>(&self, id: T::Id) -> ActorRef<T, Self>;
  async fn send<M: RequestMessageType>(&self, target: ActorId, message: M) -> M::ResponseType;
}

#[async_trait]
impl ActorSystem for LocalSystem {
  fn proxy<T: ActorType>(&self, id: T::Id) -> ActorRef<T, Self> {
    let id = id.into_id();
    let id = ActorId {
      id,
      type_id: Arc::from(T::TYPE_ID),
    };

    ActorRef {
      id,
      system: self.clone(),
      phantom: PhantomData,
    }
  }

  async fn send<M: RequestMessageType>(&self, target: ActorId, message: M) -> M::ResponseType {
    let holder = self.get_holder(target).await;
    let mut lock = holder.actor.lock_arc().await;
    let dyn_result = MessageRegistration::handle(lock.as_mut(), Box::new(message)).await;
    let casted = dyn_result
      .downcast::<M::ResponseType>()
      .unwrap_or_else(|_| panic!("failed to unwrap dyn result"));
    *casted
  }
}

impl<T: ActorType, S: ActorSystem> ActorRef<T, S> {
  async fn send<M: RequestMessageType<ActorType = T>>(&mut self, message: M) -> M::ResponseType {
    self.system.send(self.id.clone(), message).await
  }
}

mod sample;
