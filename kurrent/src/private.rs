use crate::{EventData, Streaming};

pub trait Sealed {}

impl Sealed for usize {}
impl Sealed for EventData {}
impl<A: Sealed> Sealed for Vec<A> {}
impl<I> Sealed for Streaming<I> {}
