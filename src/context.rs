//! # Gateway Context (Type-Erased State)
//!
//! This module provides a dynamic container for request-scoped state. It solves
//! the problem of passing data between decoupled middleware (e.g., Auth -> RateLimit)
//! without defining a rigid monolithic struct.
//!
//! ## Implementation Plan
//!
//! 1.  **Define `GatewayContext` Struct**:
//!     * Field: `state`: `HashMap<std::any::TypeId, Box<dyn Any + Send + Sync>>`.
//!     * We use `TypeId` as the key to allow storing one instance of any type.
//!     * We use `Box<dyn Any + Send + Sync>` to store the values thread-safely.
//!
//! 2.  **Implement Methods**:
//!     * `new() -> Self`: Initialize an empty map.
//!     * `insert<T: Send + Sync + 'static>(&mut self, val: T)`:
//!         * Calculate `TypeId::of::<T>()`.
//!         * Box the value and insert it into the map.
//!     * `get<T: 'static>(&self) -> Option<&T>`:
//!         * Calculate `TypeId::of::<T>()`.
//!         * Look up the box in the map.
//!         * Use `downcast_ref` to cast `dyn Any` back to `&T`.
use std::any::{Any, TypeId};
use std::collections::HashMap;

pub struct GatewayContext {
    state: HashMap<TypeId, Box<dyn Any + Send + Sync>>,
}

impl GatewayContext {
    pub fn new() -> Self {
        GatewayContext {
            state: HashMap::with_capacity(8),
        }
    }

    pub fn insert<T: Send + Sync + 'static>(&mut self, val: T) -> Option<T> {
        let key = TypeId::of::<T>();
        let old_boxed = self.state.insert(key, Box::new(val));
        old_boxed.map(|boxed| {
            *boxed.downcast::<T>().expect("GatewayContext: TypeId mismatch")
        })
    }

    pub fn get<T: 'static>(&self) -> Option<&T> {
        let key = TypeId::of::<T>();
        self.state.get(&key).and_then(|boxed| boxed.downcast_ref())
    }

    pub fn get_mut<T: 'static>(&mut self) -> Option<&mut T> {
        let key = TypeId::of::<T>();
        self.state.get_mut(&key).and_then(|boxed| boxed.downcast_mut())
    }

    pub fn remove<T: 'static>(&mut self) -> Option<T> {
        let key = TypeId::of::<T>();
        let removed = self.state.remove(&key);
        removed.map(|boxed| {
            *boxed.downcast::<T>().expect("GatewayContext: TypeId mismatch")
        })
    }

    pub fn exists<T: 'static>(&self) -> bool {
        let key = TypeId::of::<T>();
        self.state.contains_key(&key)
    }
}


