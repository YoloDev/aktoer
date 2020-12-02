use crate::{ActorType, RequestMessageType, ResponseMessageType};
use async_trait::async_trait;

// #[actor]
#[async_trait]
pub trait Greeter: Send + Sync {
  async fn greet(&mut self, name: String) -> String;
}

// should be output by proc macro
struct GreeterType;

// should be output by proc macro
impl ActorType for GreeterType {
  type Id = ();

  const TYPE_ID: &'static [u8] = b"Greeter";
}

// should be output by proc macro
struct GreetRequestMessage(String);

// should be output by proc macro
impl RequestMessageType for GreetRequestMessage {
  type ActorType = GreeterType;
  type ResponseType = GreetResponseMessage;

  const MESSAGE_ID: &'static [u8] = b"greet";
}

// should be output by proc macro
struct GreetResponseMessage(String);

// should be output by proc macro
impl ResponseMessageType for GreetResponseMessage {
  type RequestType = GreetRequestMessage;
}

// should be output by proc macro
const _: () = {
  use crate::{ActorRef, ActorSystem};

  #[async_trait]
  impl<S: ActorSystem> Greeter for ActorRef<GreeterType, S> {
    async fn greet(&mut self, name: String) -> String {
      let message = GreetRequestMessage(name);
      let response = self.send(message).await;
      response.0
    }
  }
};

// could be in a different crate alltogether
mod impl_in_different_crate {
  use super::{
    GreetRequestMessage, GreetResponseMessage, Greeter, GreeterType, RequestMessageType,
  };
  use crate::{Actor, ActorId};
  use async_trait::async_trait;

  struct GreeterImpl(ActorId);

  impl Actor for GreeterImpl {
    type Type = GreeterType;

    fn new(id: ActorId) -> Self {
      GreeterImpl(id)
    }
  }

  #[async_trait]
  impl Greeter for GreeterImpl {
    async fn greet(&mut self, name: String) -> String {
      format!("Hello; {}", name)
    }
  }

  // output from proc_macro
  const _: () = {
    use crate::{
      ActorRegistration, ActorType, DynActor, DynMessageHandler, DynRequestMessageType,
      DynResponseMessageType, MessageRegistration, ACTOR_TYPES, MESSAGE_TYPES,
    };
    use linkme::distributed_slice;

    #[distributed_slice(ACTOR_TYPES)]
    static GREETER_ACTOR: ActorRegistration = ActorRegistration {
      type_id: GreeterType::TYPE_ID,
      create: create_greeter,
    };

    fn create_greeter(id: ActorId) -> Box<dyn DynActor> {
      Box::from(GreeterImpl::new(id))
    }

    struct GreeterGreetHandler;

    #[async_trait]
    impl DynMessageHandler for GreeterGreetHandler {
      async fn invoke(
        &self,
        actor: &mut dyn DynActor,
        message: Box<dyn DynRequestMessageType>,
      ) -> Box<dyn DynResponseMessageType> {
        let actor = actor
          .downcast_mut::<GreeterImpl>()
          .unwrap_or_else(|| panic!("failed to unwrap dyn actor"));

        let message = message
          .downcast::<GreetRequestMessage>()
          .unwrap_or_else(|_| panic!("failed to unwrap dyn message"));

        let response = actor.greet(message.0).await;
        Box::new(GreetResponseMessage(response))
      }
    }

    #[distributed_slice(MESSAGE_TYPES)]
    static GREETER_GREET_MESSAGE: MessageRegistration = MessageRegistration {
      actor_id: GreeterType::TYPE_ID,
      message_id: GreetRequestMessage::MESSAGE_ID,
      handler: &GreeterGreetHandler,
    };
  };
}

#[async_std::test]
async fn sample_test() {
  use crate::{ActorSystem, LocalSystem};

  let system = LocalSystem::default();
  let mut greeter = system.proxy::<GreeterType>(());

  let result = greeter.greet("test".into()).await;
  assert!(false, "result: {}", result);
}
