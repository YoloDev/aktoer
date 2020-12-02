use crate::id::{ActorId, ActorTypeId};

pub struct PlacementTarget {
  actor_id: ActorId,
  type_id: ActorTypeId,
}

pub trait PlacementDirector {}
