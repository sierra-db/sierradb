# Crate Documentation

**Version:** 0.17.2

**Format Version:** 45

# Module `kameo`

# Kameo 🎬

[![Discord](https://img.shields.io/badge/Discord-5868e4?logo=discord&logoColor=white)](https://discord.gg/GMX4DV9fbk)
[![Book](https://img.shields.io/badge/Book-0B0d0e?logo=mdbook)](https://docs.page/tqwewe/kameo)
[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsors/tqwewe)
[![Crates.io Version](https://img.shields.io/crates/v/kameo)](https://crates.io/crates/kameo)
[![docs.rs](https://img.shields.io/docsrs/kameo)](https://docs.rs/kameo)
[![Crates.io Total Downloads](https://img.shields.io/crates/d/kameo)](https://crates.io/crates/kameo)
[![Crates.io License](https://img.shields.io/crates/l/kameo)](https://crates.io/crates/kameo)
[![GitHub Contributors](https://img.shields.io/github/contributors-anon/tqwewe/kameo)](https://github.com/tqwewe/kameo/graphs/contributors)

[![Kameo banner image](https://github.com/tqwewe/kameo/blob/main/docs/banner.png?raw=true)](https://github.com/tqwewe/kameo)

## Introduction

**Kameo** is a high-performance, lightweight Rust library for building fault-tolerant, asynchronous actor-based systems. Designed to scale from small, local applications to large, distributed systems, Kameo simplifies concurrent programming by providing a robust actor model that seamlessly integrates with Rust's async ecosystem.

Whether you're building a microservice, a real-time application, or an embedded system, Kameo offers the tools you need to manage concurrency, recover from failures, and scale efficiently.

## Key Features

- **Lightweight Actors**: Create actors that run in their own asynchronous tasks, leveraging Tokio for efficient concurrency.
- **Fault Tolerance**: Build resilient systems with supervision strategies that automatically recover from actor failures.
- **Flexible Messaging**: Supports both bounded and unbounded message channels, with backpressure management for load control.
- **Local and Distributed Communication**: Seamlessly send messages between actors, whether they're on the same node or across the network.
- **Panic Safety**: Actors are isolated; a panic in one actor doesn't bring down the whole system.
- **Type-Safe Interfaces**: Strong typing for messages and replies ensures compile-time correctness.
- **Easy Integration**: Compatible with existing Rust async code, and can be integrated into larger systems effortlessly.

## Why Kameo?

Kameo is designed to make concurrent programming in Rust approachable and efficient. By abstracting the complexities of async and concurrent execution, Kameo lets you focus on writing the business logic of your actors without worrying about the underlying mechanics.

Kameo is not just for distributed applications; it's equally powerful for local concurrent systems. Its flexible design allows you to start with a simple, single-node application and scale up to a distributed architecture when needed.

## Use Cases

- **Concurrent Applications**: Simplify the development of applications that require concurrency, such as web servers, data processors, or simulation engines.
- **Distributed Systems**: Build scalable microservices, distributed databases, or message brokers that require robust communication across nodes.
- **Real-Time Systems**: Ideal for applications where low-latency communication is critical, such as gaming servers, chat applications, or monitoring dashboards.
- **Embedded and IoT Devices**: Deploy lightweight actors on resource-constrained devices for efficient and reliable operation.
- **Fault-Tolerant Services**: Create services that need to remain operational even when parts of the system fail.

## Getting Started

### Installation

Add Kameo as a dependency in your `Cargo.toml` file:

```toml
[dependencies]
kameo = "0.17"
```

Alternatively, you can add it via command line:

```bash
cargo add kameo
```

## Basic Example

### Defining an Actor

```rust,ignore
use kameo::Actor;
use kameo::message::{Context, Message};

// Implement the actor
#[derive(Actor)]
struct Counter {
    count: i64,
}

// Define message
struct Inc { amount: i64 }

// Implement message handler
impl Message<Inc> for Counter {
    type Reply = i64;

    async fn handle(&mut self, msg: Inc, _ctx: &mut Context<Self, Self::Reply>) -> Self::Reply {
        self.count += msg.amount;
        self.count
    }
}
```

### Spawning and Interacting with the Actor

```rust,ignore
// Spawn the actor and obtain its reference
let actor_ref = Counter::spawn(Counter { count: 0 });

// Send messages to the actor
let count = actor_ref.ask(Inc { amount: 42 }).await?;
assert_eq!(count, 42);
```

## Distributed Actor Communication

Kameo provides built-in support for distributed actors, allowing you to send messages across network boundaries as if they were local.

### Registering an Actor

```rust,ignore
// Spawn and register the actor
let actor_ref = MyActor::spawn(MyActor::default());
actor_ref.register("my_actor").await?;
```

### Looking Up and Messaging a Remote Actor

```rust,ignore
// Lookup the remote actor
if let Some(remote_actor_ref) = RemoteActorRef::<MyActor>::lookup("my_actor").await? {
    let count = remote_actor_ref.ask(&Inc { amount: 10 }).await?;
    println!("Incremented! Count is {count}");
}
```

### Under the Hood

Kameo uses [libp2p](https://libp2p.io) for peer-to-peer networking, enabling actors to communicate over various protocols (TCP/IP, WebSockets, QUIC, etc.) using multiaddresses. This abstraction allows you to focus on your application's logic without worrying about the complexities of network programming.

## Documentation and Resources

- **[API Documentation](https://docs.rs/kameo)**: Detailed information on Kameo's API.
- **[The Kameo Book](https://docs.page/tqwewe/kameo)**: Comprehensive guide with tutorials and advanced topics.
- **[Crate on Crates.io](https://crates.io/crates/kameo)**: Latest releases and version information.
- **[Community Discord](https://discord.gg/GMX4DV9fbk)**: Join the discussion, ask questions, and share your experiences.
- **[Comparing Rust Actor Libraries](https://theari.dev/blog/comparing-rust-actor-libraries/)**: Read a blog post comparing Actix, Coerce, Kameo, Ractor, and Xtra.

## Examples

Explore more examples in the [examples](https://github.com/tqwewe/kameo/tree/main/examples) directory of the repository.

- **Basic Actor**: How to define and interact with a simple actor.
- **Distributed Actors**: Setting up actors that communicate over the network.
- **Actor Pools**: Using an actor pool with the `ActorPool` actor.
- **PubSub Actors**: Using a pubsub pattern with the `PubSub` actor.
- **Attaching Streams**: Attaching streams to an actor.

## Contributing

We welcome contributions from the community! Here are ways you can contribute:

- **Reporting Issues**: Found a bug or have a feature request? [Open an issue](https://github.com/tqwewe/kameo/issues).
- **Improving Documentation**: Help make our documentation better by submitting pull requests.
- **Contributing Code**: Check out the [CONTRIBUTING.md](https://github.com/tqwewe/kameo/blob/main/CONTRIBUTING.md) for guidelines.
- **Join the Discussion**: Participate in discussions on [Discord](https://discord.gg/GMX4DV9fbk).

## Support

[![Sponsor](https://img.shields.io/badge/sponsor-ffffff?logo=githubsponsors)](https://github.com/sponsors/tqwewe)

If you find Kameo useful and would like to support its development, please consider [sponsoring me on GitHub](https://github.com/sponsors/tqwewe). Your support helps me maintain and improve the project!

### Special Thanks to Our Sponsors

A huge thank you to [**Huly Labs**], [**Caido Community**], [**vanhouc**], and [**cawfeecoder**] for supporting Kameo's development! 💖

[![Huly Labs](https://avatars.githubusercontent.com/u/87086734?s=100&v=4)](https://huly.io/)
&nbsp;&nbsp;&nbsp;
[![Caido Community](https://avatars.githubusercontent.com/u/168573261?s=100&v=4)](https://github.com/caido-community)
&nbsp;&nbsp;&nbsp;
[![vanhouc](https://avatars.githubusercontent.com/u/3475140?s=100&v=4)](https://github.com/vanhouc)
&nbsp;&nbsp;&nbsp;
[![cawfeecoder](https://avatars.githubusercontent.com/u/10661697?s=100&v=4)](https://github.com/cawfeecoder)

[**Huly Labs**]: https://huly.io/
[**Caido Community**]: https://github.com/caido-community
[**vanhouc**]: https://github.com/vanhouc
[**cawfeecoder**]: https://github.com/cawfeecoder

## License

`kameo` is dual-licensed under either:

- MIT License ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)

You may choose either license to use this software.

---

[Introduction](#introduction) | [Key Features](#key-features) | [Why Kameo?](#why-kameo) | [Use Cases](#use-cases) | [Getting Started](#getting-started) | [Basic Example](#basic-example) | [Distributed Actor Communication](#distributed-actor-communication) | [Examples](#examples) | [Documentation](#documentation-and-resources) | [Contributing](#contributing) | [Support](#support) | [License](#license)


## Modules

## Module `actor`

Core functionality for defining and managing actors in Kameo.

Actors are independent units of computation that run asynchronously, sending and receiving messages.
Each actor operates within its own task, and its lifecycle is managed by hooks such as `on_start`, `on_panic`, and `on_stop`.

The actor trait is designed to support fault tolerance, recovery from panics, and clean termination.
Lifecycle hooks allow customization of actor behavior when starting, encountering errors, or shutting down.

This module contains the primary `Actor` trait, which all actors must implement,
as well as types for managing message queues (mailboxes) and actor references ([`ActorRef`]).

# Features
- **Asynchronous Message Handling**: Each actor processes messages asynchronously within its own task.
- **Lifecycle Hooks**: Customizable hooks ([`on_start`], [`on_stop`], [`on_panic`]) for managing the actor's lifecycle.
- **Backpressure**: Mailboxes can be bounded or unbounded, controlling the flow of messages.
- **Supervision**: Actors can be linked, enabling robust supervision and error recovery systems.

This module allows building resilient, fault-tolerant, distributed systems with flexible control over the actor lifecycle.

[`on_start`]: Actor::on_start
[`on_stop`]: Actor::on_stop
[`on_panic`]: Actor::on_panic

```rust
pub mod actor { /* ... */ }
```

### Traits

#### Trait `Actor`

Core behavior of an actor, including its lifecycle events and how it processes messages.

Every actor must implement this trait, which provides hooks
for the actor's initialization ([`on_start`]), handling errors ([`on_panic`]), and cleanup ([`on_stop`]).

The actor runs within its own task and processes messages asynchronously from a mailbox.
Each actor can be linked to others, allowing for robust supervision and failure recovery mechanisms.

Methods in this trait that return [`Self::Error`] will cause the actor to stop with the reason
[`ActorStopReason::Panicked`] if an error occurs. This enables graceful handling of actor panics
or errors.

# Example with Derive

```
use kameo::Actor;

#[derive(Actor)]
struct MyActor;
```

# Example Override Behaviour

```
use kameo::actor::{Actor, ActorRef, WeakActorRef};
use kameo::error::{ActorStopReason, Infallible};

struct MyActor;

impl Actor for MyActor {
    type Args = Self;
    type Error = Infallible;

    async fn on_start(
        state: Self::Args,
        actor_ref: ActorRef<Self>
    ) -> Result<Self, Self::Error> {
        println!("actor started");
        Ok(state)
    }

    async fn on_stop(
        &mut self,
        actor_ref: WeakActorRef<Self>,
        reason: ActorStopReason,
    ) -> Result<(), Self::Error> {
        println!("actor stopped");
        Ok(())
    }
}
```

# Lifecycle Hooks
- [`on_start`]: Called when the actor starts. This is where initialization happens.
- [`on_panic`]: Called when the actor encounters a panic or an error while processing a "tell" message.
- [`on_stop`]: Called before the actor is stopped. This allows for cleanup tasks.
- [`on_link_died`]: Hook that is invoked when a linked actor dies.

# Mailboxes
Actors use a mailbox to queue incoming messages. You can choose between:
- **Bounded Mailbox**: Limits the number of messages that can be queued, providing backpressure.
- **Unbounded Mailbox**: Allows an infinite number of messages, but can lead to high memory usage.

Mailboxes enable efficient asynchronous message passing with support for both backpressure and
unbounded queueing depending on system requirements.

[`on_start`]: Actor::on_start
[`on_panic`]: Actor::on_panic
[`on_stop`]: Actor::on_stop
[`on_link_died`]: Actor::on_link_died

```rust
pub trait Actor: Sized + Send + ''static {
    /* Associated items */
}
```

> This trait is not object-safe and cannot be used in dynamic trait objects.

##### Required Items

###### Associated Types

- `Args`: Arguments to initialize the actor.
- `Error`: Actor error type.

###### Required Methods

- `on_start`: Called when the actor starts, before it processes any messages.

##### Provided Methods

- ```rust
  fn name() -> &''static str { /* ... */ }
  ```
  The name of the actor, which can be useful for logging or debugging.

- ```rust
  fn on_message(self: &mut Self, msg: BoxMessage<Self>, actor_ref: ActorRef<Self>, tx: Option<BoxReplySender>) -> impl Future<Output = Result<(), Box<dyn ReplyError>>> + Send { /* ... */ }
  ```
  Called when the actor receives a message to be processed.

- ```rust
  fn on_panic(self: &mut Self, actor_ref: WeakActorRef<Self>, err: PanicError) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, <Self as >::Error>> + Send { /* ... */ }
  ```
  Called when the actor encounters a panic or an error during "tell" message handling.

- ```rust
  fn on_link_died(self: &mut Self, actor_ref: WeakActorRef<Self>, id: ActorId, reason: ActorStopReason) -> impl Future<Output = Result<ControlFlow<ActorStopReason>, <Self as >::Error>> + Send { /* ... */ }
  ```
  Called when a linked actor dies.

- ```rust
  fn on_stop(self: &mut Self, actor_ref: WeakActorRef<Self>, reason: ActorStopReason) -> impl Future<Output = Result<(), <Self as >::Error>> + Send { /* ... */ }
  ```
  Called before the actor stops.

- ```rust
  fn next(self: &mut Self, actor_ref: WeakActorRef<Self>, mailbox_rx: &mut MailboxReceiver<Self>) -> impl Future<Output = Option<Signal<Self>>> + Send { /* ... */ }
  ```
  Awaits the next signal typically from the mailbox.

- ```rust
  fn spawn(args: <Self as >::Args) -> ActorRef<Self> { /* ... */ }
  ```
  Spawns the actor in a Tokio task, running asynchronously with a default bounded mailbox.

- ```rust
  fn spawn_with_mailbox(args: <Self as >::Args, (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>)) -> ActorRef<Self> { /* ... */ }
  ```
  Spawns the actor in a Tokio task with a specific mailbox configuration.

- ```rust
  fn spawn_link<L>(link_ref: &ActorRef<L>, args: <Self as >::Args) -> impl Future<Output = ActorRef<Self>> + Send
where
    L: Actor { /* ... */ }
  ```
  Spawns and links the actor in a Tokio task with a default bounded mailbox.

- ```rust
  fn spawn_link_with_mailbox<L>(link_ref: &ActorRef<L>, args: <Self as >::Args, (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>)) -> impl Future<Output = ActorRef<Self>> + Send
where
    L: Actor { /* ... */ }
  ```
  Spawns and links the actor in a Tokio task with a specific mailbox configuration.

- ```rust
  fn spawn_in_thread(args: <Self as >::Args) -> ActorRef<Self> { /* ... */ }
  ```
  Spawns the actor in its own dedicated thread with a default bounded mailbox.

- ```rust
  fn spawn_in_thread_with_mailbox(args: <Self as >::Args, (mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>)) -> ActorRef<Self> { /* ... */ }
  ```
  Spawns the actor in its own dedicated thread with a specific mailbox configuration.

- ```rust
  fn prepare() -> PreparedActor<Self> { /* ... */ }
  ```
  Creates a new prepared actor, allowing access to its [`ActorRef`] before spawning.

- ```rust
  fn prepare_with_mailbox((mailbox_tx, mailbox_rx): (MailboxSender<Self>, MailboxReceiver<Self>)) -> PreparedActor<Self> { /* ... */ }
  ```
  Creates a new prepared actor with a specific mailbox configuration, allowing access to its [`ActorRef`] before spawning.

### Re-exports

#### Re-export `actor_ref::*`

```rust
pub use actor_ref::*;
```

#### Re-export `id::*`

```rust
pub use id::*;
```

#### Re-export `spawn::*`

```rust
pub use spawn::*;
```

## Module `error`

Defines error handling constructs for kameo.

This module centralizes error types used throughout kameo, encapsulating common failure scenarios encountered
in actor lifecycle management, message passing, and actor interaction. It simplifies error handling by providing
a consistent set of errors that can occur in the operation of actors and their communications.

```rust
pub mod error { /* ... */ }
```

### Types

#### Type Alias `BoxSendError`

A dyn boxed send error.

```rust
pub type BoxSendError = SendError<Box<dyn any::Any + Send>, Box<dyn any::Any + Send>>;
```

#### Enum `SendError`

Error that can occur when sending a message to an actor.

```rust
pub enum SendError<M = (), E = Infallible> {
    ActorNotRunning(M),
    ActorStopped,
    MailboxFull(M),
    HandlerError(E),
    Timeout(Option<M>),
}
```

##### Variants

###### `ActorNotRunning`

The actor isn't running.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `M` |  |

###### `ActorStopped`

The actor panicked or was stopped before a reply could be received.

###### `MailboxFull`

The actors mailbox is full.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `M` |  |

###### `HandlerError`

An error returned by the actor's message handler.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `E` |  |

###### `Timeout`

Timed out waiting for a reply.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Option<M>` |  |

##### Implementations

###### Methods

- ```rust
  pub fn map_msg<N, F>(self: Self, f: F) -> SendError<N, E>
where
    F: FnMut(M) -> N { /* ... */ }
  ```
  Maps the inner message to another type if the variant is [`ActorNotRunning`](SendError::ActorNotRunning).

- ```rust
  pub fn map_err<F, O>(self: Self, op: O) -> SendError<M, F>
where
    O: FnMut(E) -> F { /* ... */ }
  ```
  Maps the inner error to another type if the variant is [`HandlerError`](SendError::HandlerError).

- ```rust
  pub fn boxed(self: Self) -> BoxSendError
where
    M: Send + ''static,
    E: Send + ''static { /* ... */ }
  ```
  Converts the inner error types to `Box<dyn Any + Send>`.

- ```rust
  pub fn msg(self: Self) -> Option<M> { /* ... */ }
  ```
  Returns the inner message if available.

- ```rust
  pub fn err(self: Self) -> Option<E> { /* ... */ }
  ```
  Returns the inner error if available.

- ```rust
  pub fn unwrap_msg(self: Self) -> M { /* ... */ }
  ```
  Unwraps the inner message, consuming the `self` value.

- ```rust
  pub fn unwrap_err(self: Self) -> E { /* ... */ }
  ```
  Unwraps the inner handler error, consuming the `self` value.

- ```rust
  pub fn flatten(self: Self) -> SendError<M, E> { /* ... */ }
  ```
  Flattens a nested SendError.

- ```rust
  pub fn downcast<M, E>(self: Self) -> SendError<M, E>
where
    M: ''static,
    E: ''static { /* ... */ }
  ```
  Downcasts the inner error types to a concrete type.

- ```rust
  pub fn try_downcast<M, E>(self: Self) -> Result<SendError<M, E>, Self>
where
    M: ''static,
    E: ''static { /* ... */ }
  ```
  Downcasts the inner error types to a concrete type, returning an error if its the wrong type.

###### Trait Implementations

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **ReplyError**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Same**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Equivalent**
  - ```rust
    fn equivalent(self: &Self, key: &K) -> bool { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> SendError<M, E> { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Freeze**
- **Eq**
- **Unpin**
- **Instrument**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **AsTaggedExplicit**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &SendError<M, E>) -> bool { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Send**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **ErasedDestructor**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **UnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Copy**
- **StructuralPartialEq**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **Sync**
- **IntoEither**
- **WithSubscriber**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(err: mpsc::error::SendError<Signal<A>>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: mpsc::error::TrySendError<Signal<A>>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(_err: oneshot::error::RecvError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: mpsc::error::SendTimeoutError<Signal<A>>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(_: Elapsed) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: SendError<M, E>) -> Self { /* ... */ }
    ```

- **RefUnwindSafe**
- **Error**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

#### Enum `ActorStopReason`

Reason for an actor being stopped.

```rust
pub enum ActorStopReason {
    Normal,
    Killed,
    Panicked(PanicError),
    LinkDied {
        id: crate::actor::ActorId,
        reason: Box<ActorStopReason>,
    },
    PeerDisconnected,
}
```

##### Variants

###### `Normal`

Actor stopped normally.

###### `Killed`

Actor was killed.

###### `Panicked`

Actor panicked.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `PanicError` |  |

###### `LinkDied`

Link died.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `id` | `crate::actor::ActorId` | Actor ID. |
| `reason` | `Box<ActorStopReason>` | Actor died reason. |

###### `PeerDisconnected`

The peer was disconnected.

##### Implementations

###### Trait Implementations

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Sync**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **UnwindSafe**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **RefUnwindSafe**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **DeserializeOwned**
- **Same**
- **AsTaggedExplicit**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **ErasedDestructor**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> ActorStopReason { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **Unpin**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Instrument**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **ReplyError**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **WithSubscriber**
- **IntoEither**
- **AsTaggedImplicit**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Send**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Freeze**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

#### Struct `PanicError`

A shared error that occurs when an actor panics or returns an error from a hook in the [Actor] trait.

```rust
pub struct PanicError(/* private field */);
```

##### Fields

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `private` | *Private field* |

##### Implementations

###### Methods

- ```rust
  pub fn new(err: Box<dyn ReplyError>) -> Self { /* ... */ }
  ```
  Creates a new PanicError from a generic boxed reply error.

- ```rust
  pub fn with_str<F, R>(self: &Self, f: F) -> Option<R>
where
    F: FnOnce(&str) -> R { /* ... */ }
  ```
  Calls the passed closure `f` with an option containing the boxed any type downcast into a string,

- ```rust
  pub fn downcast<T>(self: &Self) -> Option<T>
where
    T: ReplyError + Clone { /* ... */ }
  ```
  Downcasts and clones the inner error, returning `Some` if the panic error matches the type `T`.

- ```rust
  pub fn with_downcast_ref<T, F, R>(self: &Self, f: F) -> Option<R>
where
    T: ReplyError,
    F: FnOnce(&T) -> R { /* ... */ }
  ```
  Calls the passed closure `f` with the inner type downcast into `T`, otherwise returns `None`.

- ```rust
  pub fn with<F, R>(self: &Self, f: F) -> R
where
    F: FnOnce(&Box<dyn ReplyError>) -> R { /* ... */ }
  ```
  Returns a reference to the error as a `&Box<dyn ReplyError>`.

###### Trait Implementations

- **Serialize**
  - ```rust
    fn serialize<S>(self: &Self, serializer: S) -> Result<<S as >::Ok, <S as >::Error>
where
    S: serde::Serializer { /* ... */ }
    ```

- **RefUnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **ReplyError**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Instrument**
- **ErasedDestructor**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **DeserializeOwned**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **AsTaggedExplicit**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> PanicError { /* ... */ }
    ```

- **IntoEither**
- **AsTaggedImplicit**
- **Sync**
- **Same**
- **UnwindSafe**
- **Freeze**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as >::Error>
where
    D: serde::Deserializer<''de> { /* ... */ }
    ```

- **WithSubscriber**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Send**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Error**
#### Enum `Infallible`

An infallible error type, similar to [std::convert::Infallible].

Kameo provides its own Infallible type in order to implement Serialize/Deserialize for it.

```rust
pub enum Infallible {
}
```

##### Variants

##### Implementations

###### Trait Implementations

- **Same**
- **UnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **IntoEither**
- **Unpin**
- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, _: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, _other: &Self) -> Option<cmp::Ordering> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Ord**
  - ```rust
    fn cmp(self: &Self, _other: &Self) -> cmp::Ordering { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **RefUnwindSafe**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Error**
  - ```rust
    fn description(self: &Self) -> &str { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **AsTaggedExplicit**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, _: &Infallible) -> bool { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, _: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Copy**
- **Comparable**
  - ```rust
    fn compare(self: &Self, key: &K) -> Ordering { /* ... */ }
    ```

- **WithSubscriber**
- **Freeze**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> Infallible { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Sync**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **ErasedDestructor**
- **Send**
- **Eq**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Hash**
  - ```rust
    fn hash<H: Hasher>(self: &Self, _: &mut H) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ReplyError**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Equivalent**
  - ```rust
    fn equivalent(self: &Self, key: &K) -> bool { /* ... */ }
    ```

- **DeserializeOwned**
#### Enum `RegistryError`

An error that can occur when registering & looking up actors by name.

```rust
pub enum RegistryError {
    SwarmNotBootstrapped,
    BadActorType,
    NameAlreadyRegistered,
    QuorumFailed {
        quorum: std::num::NonZero<usize>,
    },
    Timeout,
    Store(libp2p::kad::store::Error),
    InvalidActorRegistration(crate::remote::registry::InvalidActorRegistration),
    GetProviders(libp2p::kad::GetProvidersError),
}
```

##### Variants

###### `SwarmNotBootstrapped`

The actor swarm has not been bootstrapped.

###### `BadActorType`

The remote actor was found given the ID, but was not the correct type.

###### `NameAlreadyRegistered`

An actor has already been registered under the name.

###### `QuorumFailed`

Quorum failed.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `quorum` | `std::num::NonZero<usize>` | Required quorum. |

###### `Timeout`

Timeout.

###### `Store`

Storing the record failed.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `libp2p::kad::store::Error` |  |

###### `InvalidActorRegistration`

Invalid actor registration.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `crate::remote::registry::InvalidActorRegistration` |  |

###### `GetProviders`

Get providers error.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `libp2p::kad::GetProvidersError` |  |

##### Implementations

###### Trait Implementations

- **Sync**
- **Send**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **WithSubscriber**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Freeze**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Unpin**
- **Same**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(err: crate::remote::registry::InvalidActorRegistration) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: libp2p::kad::store::Error) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: libp2p::kad::AddProviderError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: libp2p::kad::PutRecordError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: libp2p::kad::GetProvidersError) -> Self { /* ... */ }
    ```

- **Error**
- **UnwindSafe**
- **AsTaggedExplicit**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **ErasedDestructor**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **RefUnwindSafe**
- **ReplyError**
- **Instrument**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

#### Enum `RemoteSendError`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

Error that can occur when sending a message to an actor.

```rust
pub enum RemoteSendError<E = Infallible> {
    ActorNotRunning,
    ActorStopped,
    UnknownActor {
        actor_remote_id: std::borrow::Cow<''static, str>,
    },
    UnknownMessage {
        actor_remote_id: std::borrow::Cow<''static, str>,
        message_remote_id: std::borrow::Cow<''static, str>,
    },
    BadActorType,
    MailboxFull,
    ReplyTimeout,
    HandlerError(E),
    SerializeMessage(String),
    DeserializeMessage(String),
    SerializeReply(String),
    SerializeHandlerError(String),
    DeserializeHandlerError(String),
    SwarmNotBootstrapped,
    DialFailure,
    NetworkTimeout,
    ConnectionClosed,
    UnsupportedProtocols,
    Io(Option<std::io::Error>),
}
```

##### Variants

###### `ActorNotRunning`

The actor isn't running.

###### `ActorStopped`

The actor panicked or was stopped before a reply could be received.

###### `UnknownActor`

The actor's remote ID was not found.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | The remote ID of the actor. |

###### `UnknownMessage`

The message remote ID was not found for the actor.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | The remote ID of the actor. |
| `message_remote_id` | `std::borrow::Cow<''static, str>` | The remote ID of the message. |

###### `BadActorType`

The remote actor was found given the ID, but was not the correct type.

###### `MailboxFull`

The actors mailbox is full.

###### `ReplyTimeout`

Timed out waiting for a reply.

###### `HandlerError`

An error returned by the actor's message handler.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `E` |  |

###### `SerializeMessage`

Failed to serialize the message.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `String` |  |

###### `DeserializeMessage`

Failed to deserialize the incoming message.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `String` |  |

###### `SerializeReply`

Failed to serialize the reply.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `String` |  |

###### `SerializeHandlerError`

Failed to serialize the handler error.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `String` |  |

###### `DeserializeHandlerError`

Failed to deserialize the handler error.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `String` |  |

###### `SwarmNotBootstrapped`

The actor swarm has not been bootstrapped.

###### `DialFailure`

The request could not be sent because a dialing attempt failed.

###### `NetworkTimeout`

The request timed out before a response was received.

It is not known whether the request may have been
received (and processed) by the remote peer.

###### `ConnectionClosed`

The connection closed before a response was received.

It is not known whether the request may have been
received (and processed) by the remote peer.

###### `UnsupportedProtocols`

The remote supports none of the requested protocols.

###### `Io`

An IO failure happened on an outbound stream.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Option<std::io::Error>` |  |

##### Implementations

###### Methods

- ```rust
  pub fn map_err<F, O>(self: Self, op: O) -> RemoteSendError<F>
where
    O: FnMut(E) -> F { /* ... */ }
  ```
  Maps the inner error to another type if the variant is [`HandlerError`](RemoteSendError::HandlerError).

- ```rust
  pub fn flatten(self: Self) -> RemoteSendError<E> { /* ... */ }
  ```
  Flattens a nested SendError.

###### Trait Implementations

- **Error**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **Send**
- **IntoEither**
- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Freeze**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Same**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(err: SendError<M, E>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: libp2p::request_response::OutboundFailure) -> Self { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Instrument**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **ErasedDestructor**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Sync**
- **AsTaggedExplicit**
- **ReplyError**
- **RefUnwindSafe**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **DeserializeOwned**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **UnwindSafe**
- **WithSubscriber**
#### Struct `SwarmAlreadyBootstrappedError`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

An error returned when the remote system has already been bootstrapped.

```rust
pub struct SwarmAlreadyBootstrappedError;
```

##### Implementations

###### Trait Implementations

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Freeze**
- **Copy**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **Sync**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> SwarmAlreadyBootstrappedError { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Error**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **AsTaggedImplicit**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **ErasedDestructor**
- **Same**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedExplicit**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Send**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **UnwindSafe**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **ReplyError**
- **WithSubscriber**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

### Functions

#### Function `set_actor_error_hook`

Sets a custom error hook function that's called when an actor's lifecycle hooks return an error.

This function allows you to define custom error handling behavior when an actor's
`on_start` or `on_stop` method returns an error. The hook will be called immediately
when such errors occur, regardless of whether the error is explicitly handled elsewhere.

By default, the actor system uses a hook that simply logs the error. Setting a custom
hook allows for more sophisticated error handling, such as metrics collection,
alerting, or custom logging formats.

# Parameters

* `hook`: A function that takes a reference to the error information and performs
  custom error handling.

# Example

```
use kameo::error::{set_actor_error_hook, PanicError};

// Define a custom error hook
fn my_custom_hook(err: &PanicError) {
    // log the error or something...
}

// Install the custom hook
set_actor_error_hook(my_custom_hook);
```

# Notes

* This hook is global and will affect all actors in the system.
* Setting a new hook replaces any previously set hook.
* The hook is called even if the error is also being explicitly handled via
  `wait_for_startup_result` or `wait_for_shutdown_result`.

```rust
pub fn set_actor_error_hook(hook: fn(&PanicError)) { /* ... */ }
```

## Module `mailbox`

A multi-producer, single-consumer queue for sending messages and signals between actors.

An actor mailbox is a channel which stores pending messages and signals for an actor to process sequentially.

```rust
pub mod mailbox { /* ... */ }
```

### Types

#### Enum `MailboxSender`

Sends messages and signals to the associated `MailboxReceiver`.

Instances are created by the [`bounded`] and [`unbounded`] functions.

```rust
pub enum MailboxSender<A: Actor> {
    Bounded(mpsc::Sender<Signal<A>>),
    Unbounded(mpsc::UnboundedSender<Signal<A>>),
}
```

##### Variants

###### `Bounded`

Bounded mailbox sender.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::Sender<Signal<A>>` |  |

###### `Unbounded`

Unbounded mailbox sender.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::UnboundedSender<Signal<A>>` |  |

##### Implementations

###### Methods

- ```rust
  pub async fn send(self: &Self, signal: Signal<A>) -> Result<(), mpsc::error::SendError<Signal<A>>> { /* ... */ }
  ```
  Sends a value, waiting until there is capacity.

- ```rust
  pub async fn closed(self: &Self) { /* ... */ }
  ```
  Completes when the receiver has dropped.

- ```rust
  pub fn is_closed(self: &Self) -> bool { /* ... */ }
  ```
  Checks if the channel has been closed. This happens when the

- ```rust
  pub fn same_channel(self: &Self, other: &MailboxSender<A>) -> bool { /* ... */ }
  ```
  Returns `true` if senders belong to the same channel.

- ```rust
  pub fn downgrade(self: &Self) -> WeakMailboxSender<A> { /* ... */ }
  ```
  Converts the `MailboxSender` to a [`WeakMailboxSender`] that does not count

- ```rust
  pub fn strong_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`MailboxSender`] handles.

- ```rust
  pub fn weak_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`WeakMailboxSender`] handles.

###### Trait Implementations

- **Unpin**
- **UnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Self { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Freeze**
- **WithSubscriber**
- **ErasedDestructor**
- **Instrument**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **ReplyError**
- **Same**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Send**
- **Sync**
- **AsTaggedExplicit**
#### Enum `WeakMailboxSender`

A mailbox sender that does not prevent the channel from being closed.

See tokio's [`mpsc::WeakSender`] and [`mpsc::WeakUnboundedSender`] docs for more info.

[`mpsc::WeakSender`]: tokio::sync::mpsc::WeakSender
[`mpsc::WeakUnboundedSender`]: tokio::sync::mpsc::WeakUnboundedSender

```rust
pub enum WeakMailboxSender<A: Actor> {
    Bounded(mpsc::WeakSender<Signal<A>>),
    Unbounded(mpsc::WeakUnboundedSender<Signal<A>>),
}
```

##### Variants

###### `Bounded`

Bounded weak mailbox sender.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::WeakSender<Signal<A>>` |  |

###### `Unbounded`

Unbounded weak mailbox sender.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::WeakUnboundedSender<Signal<A>>` |  |

##### Implementations

###### Methods

- ```rust
  pub fn upgrade(self: &Self) -> Option<MailboxSender<A>> { /* ... */ }
  ```
  Tries to convert a `WeakMailboxSender` into a [`MailboxSender`]. This will return `Some`

- ```rust
  pub fn strong_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`MailboxSender`] handles.

- ```rust
  pub fn weak_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`WeakMailboxSender`] handles.

###### Trait Implementations

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Freeze**
- **UnwindSafe**
- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **ReplyError**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Self { /* ... */ }
    ```

- **Instrument**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Send**
- **WithSubscriber**
- **IntoEither**
- **Unpin**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **RefUnwindSafe**
- **ErasedDestructor**
- **Same**
- **AsTaggedExplicit**
- **AsTaggedImplicit**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

#### Enum `MailboxReceiver`

Receives values from the associated `MailboxSender`.

Instances are created by the [`bounded`] and [`unbounded`] functions.

```rust
pub enum MailboxReceiver<A: Actor> {
    Bounded(mpsc::Receiver<Signal<A>>),
    Unbounded(mpsc::UnboundedReceiver<Signal<A>>),
}
```

##### Variants

###### `Bounded`

Bounded mailbox receiver.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::Receiver<Signal<A>>` |  |

###### `Unbounded`

Unbounded mailbox receiver.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `mpsc::UnboundedReceiver<Signal<A>>` |  |

##### Implementations

###### Methods

- ```rust
  pub async fn recv(self: &mut Self) -> Option<Signal<A>> { /* ... */ }
  ```
  Receives the next value for this receiver.

- ```rust
  pub async fn recv_many(self: &mut Self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize { /* ... */ }
  ```
  Receives the next values for this receiver and extends `buffer`.

- ```rust
  pub fn try_recv(self: &mut Self) -> Result<Signal<A>, TryRecvError> { /* ... */ }
  ```
  Tries to receive the next value for this receiver.

- ```rust
  pub fn blocking_recv(self: &mut Self) -> Option<Signal<A>> { /* ... */ }
  ```
  Blocking receive to call outside of asynchronous contexts.

- ```rust
  pub fn blocking_recv_many(self: &mut Self, buffer: &mut Vec<Signal<A>>, limit: usize) -> usize { /* ... */ }
  ```
  Variant of [`Self::recv_many`] for blocking contexts.

- ```rust
  pub fn close(self: &mut Self) { /* ... */ }
  ```
  Closes the receiving half of a channel, without dropping it.

- ```rust
  pub fn is_closed(self: &Self) -> bool { /* ... */ }
  ```
  Checks if a channel is closed.

- ```rust
  pub fn is_empty(self: &Self) -> bool { /* ... */ }
  ```
  Checks if a channel is empty.

- ```rust
  pub fn len(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of messages in the channel.

- ```rust
  pub fn poll_recv(self: &mut Self, cx: &mut Context<''_>) -> Poll<Option<Signal<A>>> { /* ... */ }
  ```
  Polls to receive the next message on this channel.

- ```rust
  pub fn poll_recv_many(self: &mut Self, cx: &mut Context<''_>, buffer: &mut Vec<Signal<A>>, limit: usize) -> Poll<usize> { /* ... */ }
  ```
  Polls to receive multiple messages on this channel, extending the provided buffer.

- ```rust
  pub fn sender_strong_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`MailboxSender`] handles.

- ```rust
  pub fn sender_weak_count(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of [`WeakMailboxSender`] handles.

###### Trait Implementations

- **WithSubscriber**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **UnwindSafe**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Send**
- **Freeze**
- **Unpin**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **Same**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **ReplyError**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ErasedDestructor**
- **AsTaggedExplicit**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Sync**
- **RefUnwindSafe**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

#### Enum `Signal`

**Attributes:**

- `#[allow(missing_debug_implementations)]`

A signal which can be sent to an actors mailbox.

```rust
pub enum Signal<A: Actor> {
    StartupFinished,
    Message {
        message: crate::message::BoxMessage<A>,
        actor_ref: crate::actor::ActorRef<A>,
        reply: Option<crate::reply::BoxReplySender>,
        sent_within_actor: bool,
    },
    LinkDied {
        id: crate::actor::ActorId,
        reason: crate::error::ActorStopReason,
    },
    Stop,
}
```

##### Variants

###### `StartupFinished`

The actor has finished starting up.

###### `Message`

A message.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `message` | `crate::message::BoxMessage<A>` | The boxed message. |
| `actor_ref` | `crate::actor::ActorRef<A>` | The actor ref, to keep the actor from stopping due to RAII semantics. |
| `reply` | `Option<crate::reply::BoxReplySender>` | The reply sender. |
| `sent_within_actor` | `bool` | If the message sent from within the actor's tokio task/thread |

###### `LinkDied`

A linked actor has died.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `id` | `crate::actor::ActorId` | The dead actor's ID. |
| `reason` | `crate::error::ActorStopReason` | The reason the actor stopped. |

###### `Stop`

Signals the actor to stop.

##### Implementations

###### Trait Implementations

- **Unpin**
- **IntoEither**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Sync**
- **WithSubscriber**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **AsTaggedImplicit**
- **ErasedDestructor**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Instrument**
- **Same**
- **Send**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedExplicit**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **RefUnwindSafe**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Freeze**
- **UnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

### Functions

#### Function `bounded`

Creates a bounded mailbox for communicating between actors with backpressure.

_See tokio's [`mpsc::channel`] docs for more info._

[`mpsc::channel`]: tokio::sync::mpsc::channel

```rust
pub fn bounded<A: Actor>(buffer: usize) -> (MailboxSender<A>, MailboxReceiver<A>) { /* ... */ }
```

#### Function `unbounded`

Creates an unbounded mailbox for communicating between actors without backpressure.

See tokio's [`mpsc::unbounded_channel`] docs for more info.

[`mpsc::unbounded_channel`]: tokio::sync::mpsc::unbounded_channel

```rust
pub fn unbounded<A: Actor>() -> (MailboxSender<A>, MailboxReceiver<A>) { /* ... */ }
```

## Module `message`

Messaging infrastructure for actor communication in Kameo.

This module provides the constructs necessary for handling messages within Kameo,
defining how actors communicate and interact. It equips actors with the ability to receive and respond
to both commands that might change their internal state and requests for information which do not alter their state.

A key component of this module is the [`Context`], which is passed to message handlers, offering them a
reference to the current actor and a way to reply to messages. This enables actors to perform a wide range of
actions in response to received messages, from altering their own state to querying other actors.

The module distinguishes between two kinds of communication: messages, which are intended to modify an actor's
state and might lead to side effects, and queries, which are read-only requests for information from an actor.
This distinction helps in clearly separating commands from queries, aligning with the CQRS
(Command Query Responsibility Segregation) principle and enhancing the clarity and maintainability of actor
interactions. It also provides some performance benefits in that sequential queries can be processed concurrently.

```rust
pub mod message { /* ... */ }
```

### Types

#### Type Alias `BoxMessage`

A boxed dynamic message type for the actor `A`.

```rust
pub type BoxMessage<A> = Box<dyn DynMessage<A>>;
```

#### Type Alias `BoxReply`

A boxed dynamic type used for message replies.

```rust
pub type BoxReply = Box<dyn any::Any + Send>;
```

#### Enum `StreamMessage`

A type for handling streams attached to an actor.

Actors which implement handling messages of this type can receive and process messages from a stream attached to the actor.
This type is designed to facilitate the integration of streaming data sources with actors,
allowing actors to react and process each message as it arrives from the stream.

It's typically used with [ActorRef::attach_stream] to attach a stream to an actor.

```rust
pub enum StreamMessage<T, S, F> {
    Next(T),
    Started(S),
    Finished(F),
}
```

##### Variants

###### `Next`

The next item in a stream.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `T` |  |

###### `Started`

The stream has just been attached.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `S` |  |

###### `Finished`

The stream has finished, and no more items will be sent.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `F` |  |

##### Implementations

###### Trait Implementations

- **ReplyError**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamMessage<T, S, F> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Freeze**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Send**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **AsTaggedImplicit**
- **ErasedDestructor**
- **Same**
- **UnwindSafe**
- **Unpin**
- **Instrument**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedExplicit**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **WithSubscriber**
- **RefUnwindSafe**
- **IntoEither**
- **Sync**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

#### Struct `Context`

A context provided to message handlers providing access
to the current actor ref, and reply channel.

```rust
pub struct Context<A, R> {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Methods

- ```rust
  pub fn actor_ref(self: &Self) -> ActorRef<A> { /* ... */ }
  ```
  Returns the current actor's ref, allowing messages to be sent to itself.

- ```rust
  pub fn reply_sender(self: &mut Self) -> (DelegatedReply<<R as >::Value>, Option<ReplySender<<R as >::Value>>) { /* ... */ }
  ```
  Extracts the reply sender, providing a mechanism for delegated responses and an optional reply sender.

- ```rust
  pub fn reply(self: &mut Self, reply: <R as >::Value) -> DelegatedReply<<R as >::Value> { /* ... */ }
  ```
  Sends a reply to the caller early, returning a `DelegatedReply`.

- ```rust
  pub async fn forward<B, M>(self: &mut Self, actor_ref: &ActorRef<B>, message: M) -> ForwardedReply<M, <B as Message<M>>::Reply>
where
    B: Message<M>,
    M: Send + ''static { /* ... */ }
  ```
  Forwards the message to another actor, returning a [ForwardedReply].

- ```rust
  pub fn try_forward<B, M>(self: &mut Self, actor_ref: &ActorRef<B>, message: M) -> ForwardedReply<M, <B as Message<M>>::Reply>
where
    B: Message<M>,
    M: Send + ''static { /* ... */ }
  ```
  Tries to forward the message to another actor, returning a [ForwardedReply],

- ```rust
  pub fn blocking_forward<B, M>(self: &mut Self, actor_ref: &ActorRef<B>, message: M) -> ForwardedReply<M, <B as Message<M>>::Reply>
where
    B: Message<M>,
    M: Send + ''static { /* ... */ }
  ```
  Forwards the message to another actor, returning a [ForwardedReply].

###### Trait Implementations

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **ReplyError**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **WithSubscriber**
- **IntoEither**
- **AsTaggedExplicit**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **Same**
- **Unpin**
- **Freeze**
- **Send**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **UnwindSafe**
- **ErasedDestructor**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

### Traits

#### Trait `Message`

A message that can modify an actors state.

Messages are processed sequentially one at a time, with exclusive mutable access to the actors state.

The reply type must implement [Reply].

```rust
pub trait Message<T: Send + ''static>: Actor {
    /* Associated items */
}
```

> This trait is not object-safe and cannot be used in dynamic trait objects.

##### Required Items

###### Associated Types

- `Reply`: The reply sent back to the message caller.

###### Required Methods

- `handle`: Handler for this message.

#### Trait `DynMessage`

An object safe message which can be handled by an actor `A`.

This trait is implemented for all types which implement [`Message`], and is typically used for advanced cases such
as buffering actor messages.

```rust
pub trait DynMessage<A>
where
    Self: Send,
    A: Actor {
    /* Associated items */
}
```

##### Required Items

###### Required Methods

- `handle_dyn`: Handles the dyn message with the provided actor state, ref, and reply sender.
- `as_any`: Casts the type to a `Box<dyn Any>`.

##### Implementations

This trait is implemented for the following types:

- `T` with <A, T>

## Module `remote`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

# Remote Actors in Kameo

The `remote` module in Kameo provides tools for managing distributed actors across nodes,
enabling actors to communicate seamlessly in a peer-to-peer (P2P) network. By leveraging
the [libp2p](https://libp2p.io) library, Kameo allows you to register actors under unique
names and send messages between actors on different nodes as though they were local.

## Key Features

- **Composable Architecture**: The [`Behaviour`] struct implements libp2p's `NetworkBehaviour`,
  allowing seamless integration with existing libp2p applications and other protocols.
- **Quick Bootstrap**: The [`bootstrap()`] and [`bootstrap_on()`] functions provide one-line
  setup for development and simple deployments.
- **Actor Registration & Discovery**: Actors can be registered under unique names and looked up
  across the network using [`RemoteActorRef`](crate::actor::RemoteActorRef).
- **Reliable Messaging**: Ensures reliable message delivery between nodes using a combination
  of Kademlia DHT for discovery and request-response protocols for communication.
- **Modular Design**: Separate [`messaging`] and [`registry`] modules handle different aspects
  of distributed actor communication.

## Getting Started

For quick prototyping and development:

```
use kameo::remote;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One line to bootstrap a distributed actor system
    let peer_id = remote::bootstrap()?;
     
    // Now use actors normally
    // actor_ref.register("my_actor").await?;
     
    Ok(())
}
```

For production deployments with custom configuration:

```no_run
use kameo::remote;
use libp2p::swarm::NetworkBehaviour;

#[derive(NetworkBehaviour)]
struct MyBehaviour {
    kameo: remote::Behaviour,
    // Add other libp2p behaviors as needed
}

// Create custom libp2p swarm with full control over
// transports, discovery, and protocol composition
```

```rust
pub mod remote { /* ... */ }
```

### Modules

## Module `messaging`

Remote message passing infrastructure for actors across the network.

This module provides the core messaging capabilities that enable actors running on different
nodes to communicate with each other seamlessly. It handles the serialization, routing, and
delivery of messages between remote actors while maintaining the same ergonomics as local
actor communication.

# Key Responsibilities

- **Message Serialization**: Automatically serializes and deserializes actor messages for
  network transmission using efficient binary protocols
- **Request-Response Communication**: Implements reliable ask/tell patterns for remote actors
  with configurable timeouts and delivery guarantees  
- **Connection Management**: Manages libp2p connections and handles connection failures,
  retries, and peer disconnections gracefully
- **Actor Lifecycle Integration**: Supports remote actor linking, unlinking, and death
  notifications across network boundaries
- **Backpressure Handling**: Provides mailbox timeout controls to prevent overwhelming
  remote actors with too many concurrent messages

# Architecture

The messaging system is built on top of libp2p's request-response protocol, providing
reliable delivery semantics while maintaining the actor model's message-passing paradigm.
Messages are automatically routed to the appropriate peer based on the target actor's
peer ID, with transparent handling of network-level concerns.

The module integrates closely with Kameo's local actor system, allowing remote actors
to be used interchangeably with local actors through the same `ActorRef` interface.

```rust
pub mod messaging { /* ... */ }
```

### Types

#### Enum `RequestId`

Identifier for a request within the swarm behavior.

Requests can be either local (handled within the same peer) or outbound
(sent to remote peers via libp2p's request-response protocol).

```rust
pub enum RequestId {
    Local(u64),
    Outbound(request_response::OutboundRequestId),
}
```

##### Variants

###### `Local`

A local request handled within the same peer.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u64` |  |

###### `Outbound`

An outbound request sent to a remote peer.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `request_response::OutboundRequestId` |  |

##### Implementations

###### Trait Implementations

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **ErasedDestructor**
- **Unpin**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **IntoEither**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Instrument**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Send**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Freeze**
- **ReplyError**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &RequestId) -> bool { /* ... */ }
    ```

  - ```rust
    fn eq(self: &Self, other: &request_response::OutboundRequestId) -> bool { /* ... */ }
    ```

  - ```rust
    fn eq(self: &Self, other: &RequestId) -> bool { /* ... */ }
    ```

- **WithSubscriber**
- **Same**
- **AsTaggedImplicit**
- **Copy**
- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, other: &RequestId) -> $crate::option::Option<$crate::cmp::Ordering> { /* ... */ }
    ```

- **Ord**
  - ```rust
    fn cmp(self: &Self, other: &RequestId) -> $crate::cmp::Ordering { /* ... */ }
    ```

- **StructuralPartialEq**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Equivalent**
  - ```rust
    fn equivalent(self: &Self, key: &K) -> bool { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Sync**
- **Comparable**
  - ```rust
    fn compare(self: &Self, key: &K) -> Ordering { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **UnwindSafe**
- **Eq**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Hash**
  - ```rust
    fn hash<__H: $crate::hash::Hasher>(self: &Self, state: &mut __H) { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> RequestId { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **AsTaggedExplicit**
- **RefUnwindSafe**
#### Enum `SwarmRequest`

Represents different types of requests that can be made within the swarm.

```rust
pub enum SwarmRequest {
    Ask {
        actor_id: crate::actor::ActorId,
        actor_remote_id: std::borrow::Cow<''static, str>,
        message_remote_id: std::borrow::Cow<''static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<std::time::Duration>,
        reply_timeout: Option<std::time::Duration>,
        immediate: bool,
    },
    Tell {
        actor_id: crate::actor::ActorId,
        actor_remote_id: std::borrow::Cow<''static, str>,
        message_remote_id: std::borrow::Cow<''static, str>,
        payload: Vec<u8>,
        mailbox_timeout: Option<std::time::Duration>,
        immediate: bool,
    },
    Link {
        actor_id: crate::actor::ActorId,
        actor_remote_id: std::borrow::Cow<''static, str>,
        sibbling_id: crate::actor::ActorId,
        sibbling_remote_id: std::borrow::Cow<''static, str>,
    },
    Unlink {
        actor_id: crate::actor::ActorId,
        actor_remote_id: std::borrow::Cow<''static, str>,
        sibbling_id: crate::actor::ActorId,
    },
    SignalLinkDied {
        dead_actor_id: crate::actor::ActorId,
        notified_actor_id: crate::actor::ActorId,
        notified_actor_remote_id: std::borrow::Cow<''static, str>,
        stop_reason: crate::error::ActorStopReason,
    },
}
```

##### Variants

###### `Ask`

Represents a request to ask a peer for some data or action.

This variant includes information about the actor, the message, payload, and timeout settings.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_id` | `crate::actor::ActorId` | Identifier of the actor initiating the request. |
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | Remote identifier of the actor as a static string. |
| `message_remote_id` | `std::borrow::Cow<''static, str>` | Remote identifier of the message as a static string. |
| `payload` | `Vec<u8>` | The payload data to be sent with the request. |
| `mailbox_timeout` | `Option<std::time::Duration>` | Optional timeout duration for the mailbox to receive the request. |
| `reply_timeout` | `Option<std::time::Duration>` | Optional timeout duration to wait for a reply to the request. |
| `immediate` | `bool` | Indicates whether the request should be sent immediately. |

###### `Tell`

Represents a request to tell a peer some information without expecting a response.

This variant includes information about the actor, the message, payload, and timeout settings.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_id` | `crate::actor::ActorId` | Identifier of the actor initiating the message. |
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | Remote identifier of the actor as a static string. |
| `message_remote_id` | `std::borrow::Cow<''static, str>` | Remote identifier of the message as a static string. |
| `payload` | `Vec<u8>` | The payload data to be sent with the message. |
| `mailbox_timeout` | `Option<std::time::Duration>` | Optional timeout duration for the mailbox to receive the message. |
| `immediate` | `bool` | Indicates whether the message should be sent immediately. |

###### `Link`

A request to link two actors together.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_id` | `crate::actor::ActorId` | Actor ID. |
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | Actor remote ID. |
| `sibbling_id` | `crate::actor::ActorId` | Sibbling ID. |
| `sibbling_remote_id` | `std::borrow::Cow<''static, str>` | Sibbling remote ID. |

###### `Unlink`

A request to unlink two actors.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `actor_id` | `crate::actor::ActorId` | Actor ID. |
| `actor_remote_id` | `std::borrow::Cow<''static, str>` | Actor remote ID. |
| `sibbling_id` | `crate::actor::ActorId` | Sibbling ID. |

###### `SignalLinkDied`

A signal notifying a linked actor has died.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `dead_actor_id` | `crate::actor::ActorId` | The actor which died. |
| `notified_actor_id` | `crate::actor::ActorId` | The actor to notify. |
| `notified_actor_remote_id` | `std::borrow::Cow<''static, str>` | The actor to notify. |
| `stop_reason` | `crate::error::ActorStopReason` | The reason the actor died. |

##### Implementations

###### Trait Implementations

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Freeze**
- **Same**
- **ErasedDestructor**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Send**
- **AsTaggedImplicit**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Unpin**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **ReplyError**
- **Sync**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DeserializeOwned**
- **UnwindSafe**
- **WithSubscriber**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **IntoEither**
- **RefUnwindSafe**
- **AsTaggedExplicit**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

#### Enum `SwarmResponse`

Represents different types of responses that can be sent within the swarm.

```rust
pub enum SwarmResponse {
    Ask(Result<Vec<u8>, crate::error::RemoteSendError<Vec<u8>>>),
    Tell(Result<(), crate::error::RemoteSendError>),
    Link(Result<(), crate::error::RemoteSendError>),
    Unlink(Result<(), crate::error::RemoteSendError>),
    SignalLinkDied(Result<(), crate::error::RemoteSendError>),
    OutboundFailure(crate::error::RemoteSendError),
}
```

##### Variants

###### `Ask`

Represents the response to an `Ask` request.

Contains either the successful payload data or an error indicating why the send failed.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Result<Vec<u8>, crate::error::RemoteSendError<Vec<u8>>>` |  |

###### `Tell`

Represents the response to a `Tell` request.

Contains either a successful acknowledgment or an error indicating why the send failed.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Result<(), crate::error::RemoteSendError>` |  |

###### `Link`

Represents the response to a link request.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Result<(), crate::error::RemoteSendError>` |  |

###### `Unlink`

Represents the response to a link request.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Result<(), crate::error::RemoteSendError>` |  |

###### `SignalLinkDied`

Represents the response to a link died signal.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Result<(), crate::error::RemoteSendError>` |  |

###### `OutboundFailure`

Represents a failure that occurred while attempting to send an outbound request.

Contains the error that caused the outbound request to fail.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `crate::error::RemoteSendError` |  |

##### Implementations

###### Trait Implementations

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **UnwindSafe**
- **Instrument**
- **WithSubscriber**
- **Same**
- **AsTaggedImplicit**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **ReplyError**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Send**
- **Unpin**
- **RefUnwindSafe**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **IntoEither**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **AsTaggedExplicit**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **ErasedDestructor**
- **DeserializeOwned**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

#### Enum `Event`

The events produced by the `Messaging` behaviour.

See [`NetworkBehaviour::poll`].

```rust
pub enum Event {
    AskResult {
        peer: libp2p::PeerId,
        connection_id: Option<libp2p::swarm::ConnectionId>,
        request_id: RequestId,
        result: Result<Vec<u8>, crate::error::RemoteSendError<Vec<u8>>>,
    },
    TellResult {
        peer: libp2p::PeerId,
        connection_id: Option<libp2p::swarm::ConnectionId>,
        request_id: RequestId,
        result: Result<(), crate::error::RemoteSendError>,
    },
    LinkResult {
        peer: libp2p::PeerId,
        connection_id: Option<libp2p::swarm::ConnectionId>,
        request_id: RequestId,
        result: Result<(), crate::error::RemoteSendError>,
    },
    UnlinkResult {
        peer: libp2p::PeerId,
        connection_id: Option<libp2p::swarm::ConnectionId>,
        request_id: RequestId,
        result: Result<(), crate::error::RemoteSendError>,
    },
    SignalLinkDiedResult {
        peer: libp2p::PeerId,
        connection_id: Option<libp2p::swarm::ConnectionId>,
        request_id: RequestId,
        result: Result<(), crate::error::RemoteSendError>,
    },
    OutboundFailure {
        peer: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        request_id: request_response::OutboundRequestId,
        error: crate::error::RemoteSendError,
    },
    InboundFailure {
        peer: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        request_id: request_response::InboundRequestId,
        error: request_response::InboundFailure,
    },
    ResponseSent {
        peer: libp2p::PeerId,
        connection_id: libp2p::swarm::ConnectionId,
        request_id: request_response::InboundRequestId,
    },
}
```

##### Variants

###### `AskResult`

Result of an ask request to a remote actor.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer that handled the request. |
| `connection_id` | `Option<libp2p::swarm::ConnectionId>` | The connection used, if any. |
| `request_id` | `RequestId` | The request ID. |
| `result` | `Result<Vec<u8>, crate::error::RemoteSendError<Vec<u8>>>` | The result of the ask operation. |

###### `TellResult`

Result of a tell message to a remote actor.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer that handled the message. |
| `connection_id` | `Option<libp2p::swarm::ConnectionId>` | The connection used, if any. |
| `request_id` | `RequestId` | The request ID. |
| `result` | `Result<(), crate::error::RemoteSendError>` | The result of the tell operation. |

###### `LinkResult`

Result of a link operation between actors.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer that handled the link. |
| `connection_id` | `Option<libp2p::swarm::ConnectionId>` | The connection used, if any. |
| `request_id` | `RequestId` | The request ID. |
| `result` | `Result<(), crate::error::RemoteSendError>` | The result of the link operation. |

###### `UnlinkResult`

Result of an unlink operation between actors.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer that handled the unlink. |
| `connection_id` | `Option<libp2p::swarm::ConnectionId>` | The connection used, if any. |
| `request_id` | `RequestId` | The request ID. |
| `result` | `Result<(), crate::error::RemoteSendError>` | The result of the unlink operation. |

###### `SignalLinkDiedResult`

Result of signaling that a linked actor died.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer that handled the signal. |
| `connection_id` | `Option<libp2p::swarm::ConnectionId>` | The connection used, if any. |
| `request_id` | `RequestId` | The request ID. |
| `result` | `Result<(), crate::error::RemoteSendError>` | The result of the signal operation. |

###### `OutboundFailure`

An outbound request failed.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer to whom the request was sent. |
| `connection_id` | `libp2p::swarm::ConnectionId` | The connection used. |
| `request_id` | `request_response::OutboundRequestId` | The (local) ID of the failed request. |
| `error` | `crate::error::RemoteSendError` | The error that occurred. |

###### `InboundFailure`

An inbound request failed.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer from whom the request was received. |
| `connection_id` | `libp2p::swarm::ConnectionId` | The connection used. |
| `request_id` | `request_response::InboundRequestId` | The ID of the failed inbound request. |
| `error` | `request_response::InboundFailure` | The error that occurred. |

###### `ResponseSent`

A response to an inbound request has been sent.

When this event is received, the response has been flushed on
the underlying transport connection.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The peer to whom the response was sent. |
| `connection_id` | `libp2p::swarm::ConnectionId` | The connection used. |
| `request_id` | `request_response::InboundRequestId` | The ID of the inbound request whose response was sent. |

##### Implementations

###### Trait Implementations

- **Unpin**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Send**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **IntoEither**
- **Sync**
- **ReplyError**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **RefUnwindSafe**
- **Same**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ErasedDestructor**
- **WithSubscriber**
- **UnwindSafe**
- **Freeze**
- **Instrument**
- **AsTaggedExplicit**
#### Struct `Config`

The configuration for a `messaging::Behaviour` protocol.

```rust
pub struct Config {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Methods

- ```rust
  pub fn with_request_timeout(self: Self, v: Duration) -> Self { /* ... */ }
  ```
  Sets the timeout for inbound and outbound requests.

- ```rust
  pub fn with_max_concurrent_streams(self: Self, num_streams: usize) -> Self { /* ... */ }
  ```
  Sets the upper bound for the number of concurrent inbound + outbound streams.

- ```rust
  pub fn with_request_size_maximum(self: Self, bytes: u64) -> Self { /* ... */ }
  ```
  Sets the limit for request size in bytes.

- ```rust
  pub fn with_response_size_maximum(self: Self, bytes: u64) -> Self { /* ... */ }
  ```
  Sets the limit for response size in bytes.

###### Trait Implementations

- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **ReplyError**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Same**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(config: Config) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(config: Config) -> Self { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Config { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **ErasedDestructor**
- **Sync**
- **WithSubscriber**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **AsTaggedExplicit**
- **RefUnwindSafe**
- **IntoEither**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Send**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Copy**
- **UnwindSafe**
- **Instrument**
- **Default**
  - ```rust
    fn default() -> Self { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

#### Struct `Behaviour`

**Attributes:**

- `#[allow(missing_debug_implementations)]`

`Behaviour` is a `NetworkBehaviour` that implements the kameo messaging behaviour
on top of the request response protocol.

```rust
pub struct Behaviour {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Methods

- ```rust
  pub fn new(local_peer_id: PeerId, config: Config) -> Self { /* ... */ }
  ```
  Creates a new messaging behaviour.

- ```rust
  pub fn ask(self: &mut Self, actor_id: ActorId, actor_remote_id: Cow<''static, str>, message_remote_id: Cow<''static, str>, payload: Vec<u8>, mailbox_timeout: Option<Duration>, reply_timeout: Option<Duration>, immediate: bool) -> RequestId { /* ... */ }
  ```
  Sends an ask request to a remote actor.

- ```rust
  pub fn tell(self: &mut Self, actor_id: ActorId, actor_remote_id: Cow<''static, str>, message_remote_id: Cow<''static, str>, payload: Vec<u8>, mailbox_timeout: Option<Duration>, immediate: bool) -> RequestId { /* ... */ }
  ```
  Sends a tell message to a remote actor.

- ```rust
  pub fn link(self: &mut Self, actor_id: ActorId, actor_remote_id: Cow<''static, str>, sibbling_id: ActorId, sibbling_remote_id: Cow<''static, str>) -> RequestId { /* ... */ }
  ```
  Creates a link between two actors across the network.

- ```rust
  pub fn unlink(self: &mut Self, actor_id: ActorId, actor_remote_id: Cow<''static, str>, sibbling_id: ActorId) -> RequestId { /* ... */ }
  ```
  Removes a link between two actors across the network.

- ```rust
  pub fn signal_link_died(self: &mut Self, dead_actor_id: ActorId, notified_actor_id: ActorId, notified_actor_remote_id: Cow<''static, str>, stop_reason: ActorStopReason) -> RequestId { /* ... */ }
  ```
  Signals that a linked actor has died to another actor.

###### Trait Implementations

- **UnwindSafe**
- **ErasedDestructor**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **IntoEither**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Instrument**
- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **NetworkBehaviour**
  - ```rust
    fn handle_established_inbound_connection(self: &mut Self, connection_id: ConnectionId, peer: libp2p::PeerId, local_addr: &libp2p::Multiaddr, remote_addr: &libp2p::Multiaddr) -> Result<THandler<Self>, ConnectionDenied> { /* ... */ }
    ```

  - ```rust
    fn handle_established_outbound_connection(self: &mut Self, connection_id: ConnectionId, peer: libp2p::PeerId, addr: &libp2p::Multiaddr, role_override: libp2p::core::Endpoint, port_use: libp2p::core::transport::PortUse) -> Result<THandler<Self>, ConnectionDenied> { /* ... */ }
    ```

  - ```rust
    fn on_swarm_event(self: &mut Self, event: FromSwarm<''_>) { /* ... */ }
    ```

  - ```rust
    fn on_connection_handler_event(self: &mut Self, peer_id: libp2p::PeerId, connection_id: ConnectionId, event: THandlerOutEvent<Self>) { /* ... */ }
    ```

  - ```rust
    fn poll(self: &mut Self, cx: &mut task::Context<''_>) -> task::Poll<ToSwarm<<Self as >::ToSwarm, THandlerInEvent<Self>>> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **WithSubscriber**
- **RefUnwindSafe**
- **Send**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Same**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Unpin**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Freeze**
- **AsTaggedExplicit**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

## Module `registry`

Actor registration and discovery system for distributed actor networks.

This module implements a distributed registry that allows actors to register themselves
under human-readable names and be discovered by other actors across the network. It uses
Kademlia DHT (Distributed Hash Table) for decentralized storage and lookup of actor
registrations, ensuring high availability and fault tolerance.

# Key Responsibilities

- **Actor Registration**: Enables actors to register themselves under string-based names,
  making them discoverable by other actors across the network
- **Distributed Discovery**: Provides lookup capabilities to find all actors registered
  under a given name, regardless of which peer they're running on
- **Metadata Storage**: Stores actor metadata including unique identifiers, peer locations,
  and other registration details in a distributed manner
- **Network Resilience**: Uses Kademlia's distributed nature to ensure actor registrations
  remain available even if some network nodes go offline
- **Local Caching**: Maintains local caches of discovered actors to reduce lookup latency
  and network overhead for frequently accessed actors

# Architecture

The registry leverages libp2p's Kademlia implementation to create a distributed hash table
where actor names serve as keys and actor metadata serves as values. Each actor registration
involves two operations: advertising the actor name as a "provider" and storing the actor's
detailed metadata as a record in the DHT.

This dual approach enables efficient discovery where clients first find all peers providing
a given actor name, then retrieve the specific metadata for each instance. This supports
scenarios where multiple actors may be registered under the same logical name across
different peers for load balancing or redundancy.

```rust
pub mod registry { /* ... */ }
```

### Types

#### Enum `Event`

The events produced by the `Registry` behaviour.

See [`NetworkBehaviour::poll`].

```rust
pub enum Event {
    LookupProgressed {
        provider_query_id: kad::QueryId,
        get_query_id: kad::QueryId,
        result: Result<ActorRegistration<''static>, crate::error::RegistryError>,
    },
    LookupTimeout {
        provider_query_id: kad::QueryId,
    },
    LookupCompleted {
        provider_query_id: kad::QueryId,
    },
    RegistrationFailed {
        provider_query_id: kad::QueryId,
        error: crate::error::RegistryError,
    },
    RegisteredActor {
        provider_result: kad::AddProviderResult,
        provider_query_id: kad::QueryId,
        metadata_result: kad::PutRecordResult,
        metadata_query_id: kad::QueryId,
    },
    RoutingUpdated {
        peer: libp2p::PeerId,
        is_new_peer: bool,
        addresses: kad::Addresses,
        bucket_range: (kad::KBucketDistance, kad::KBucketDistance),
        old_peer: Option<libp2p::PeerId>,
    },
    UnroutablePeer {
        peer: libp2p::PeerId,
    },
    RoutablePeer {
        peer: libp2p::PeerId,
        address: libp2p::Multiaddr,
    },
    PendingRoutablePeer {
        peer: libp2p::PeerId,
        address: libp2p::Multiaddr,
    },
}
```

##### Variants

###### `LookupProgressed`

Progress in looking up actors by name.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `provider_query_id` | `kad::QueryId` | The original provider query ID. |
| `get_query_id` | `kad::QueryId` | The metadata query ID. |
| `result` | `Result<ActorRegistration<''static>, crate::error::RegistryError>` | The actor registration found, or an error. |

###### `LookupTimeout`

A lookup operation timed out while searching for providers.
More metadata might still arrive from providers found before timeout.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `provider_query_id` | `kad::QueryId` | The provider query ID that timed out. |

###### `LookupCompleted`

A lookup operation completed. This is always the final event for a lookup.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `provider_query_id` | `kad::QueryId` | The completed provider query ID. |

###### `RegistrationFailed`

An actor registration failed.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `provider_query_id` | `kad::QueryId` | The provider query ID that failed. |
| `error` | `crate::error::RegistryError` | The error that caused the failure. |

###### `RegisteredActor`

An actor was successfully registered.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `provider_result` | `kad::AddProviderResult` | The result of the provider registration. |
| `provider_query_id` | `kad::QueryId` | The provider query ID. |
| `metadata_result` | `kad::PutRecordResult` | The result of storing metadata. |
| `metadata_query_id` | `kad::QueryId` | The metadata query ID. |

###### `RoutingUpdated`

The routing table has been updated with a new peer and / or
address, thereby possibly evicting another peer.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | The ID of the peer that was added or updated. |
| `is_new_peer` | `bool` | Whether this is a new peer and was thus just added to the routing<br>table, or whether it is an existing peer who's addresses changed. |
| `addresses` | `kad::Addresses` | The full list of known addresses of `peer`. |
| `bucket_range` | `(kad::KBucketDistance, kad::KBucketDistance)` | Returns the minimum inclusive and maximum inclusive distance for<br>the bucket of the peer. |
| `old_peer` | `Option<libp2p::PeerId>` | The ID of the peer that was evicted from the routing table to make<br>room for the new peer, if any. |

###### `UnroutablePeer`

A peer has connected for whom no listen address is known.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | Peer ID. |

###### `RoutablePeer`

A connection to a peer has been established for whom a listen address
is known but the peer has not been added to the routing table.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | Peer ID. |
| `address` | `libp2p::Multiaddr` | Address. |

###### `PendingRoutablePeer`

A connection to a peer has been established for whom a listen address
is known but the peer is only pending insertion into the routing table
if the least-recently disconnected peer is unresponsive, i.e. the peer
may not make it into the routing table.

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `peer` | `libp2p::PeerId` | Peer ID. |
| `address` | `libp2p::Multiaddr` | Address. |

##### Implementations

###### Trait Implementations

- **AsTaggedExplicit**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Unpin**
- **UnwindSafe**
- **Sync**
- **Freeze**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **WithSubscriber**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Same**
- **IntoEither**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ReplyError**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **ErasedDestructor**
- **RefUnwindSafe**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Instrument**
- **Send**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

#### Struct `Behaviour`

**Attributes:**

- `#[allow(missing_debug_implementations)]`

`Behaviour` is a `NetworkBehaviour` that implements the kameo registry behaviour
on top of the Kademlia protocol.

```rust
pub struct Behaviour {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Methods

- ```rust
  pub fn new(local_peer_id: PeerId) -> Self { /* ... */ }
  ```
  Creates a new registry behaviour.

- ```rust
  pub fn register(self: &mut Self, name: String, registration: ActorRegistration<''static>) -> Result<kad::QueryId, kad::store::Error> { /* ... */ }
  ```
  Registers an actor in the distributed registry.

- ```rust
  pub fn cancel_registration(self: &mut Self, query_id: &kad::QueryId) -> bool { /* ... */ }
  ```
  Cancels an ongoing actor registration.

- ```rust
  pub fn unregister(self: &mut Self, name: &str) { /* ... */ }
  ```
  Unregisters an actor from the distributed registry.

- ```rust
  pub fn lookup(self: &mut Self, name: String) -> kad::QueryId { /* ... */ }
  ```
  Looks up actors by name in the distributed registry.

- ```rust
  pub fn lookup_local(self: &mut Self, name: &str) -> Result<Option<ActorRegistration<''static>>, RegistryError> { /* ... */ }
  ```
  Looks up an actor in the local registry only.

- ```rust
  pub fn cancel_lookup(self: &mut Self, query_id: &kad::QueryId) -> bool { /* ... */ }
  ```
  Cancels an ongoing actor lookup.

###### Trait Implementations

- **Same**
- **AsTaggedExplicit**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **ErasedDestructor**
- **AsTaggedImplicit**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Send**
- **Instrument**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Freeze**
- **Unpin**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **WithSubscriber**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **NetworkBehaviour**
  - ```rust
    fn handle_established_inbound_connection(self: &mut Self, connection_id: libp2p::swarm::ConnectionId, peer: PeerId, local_addr: &libp2p::Multiaddr, remote_addr: &libp2p::Multiaddr) -> Result<THandler<Self>, ConnectionDenied> { /* ... */ }
    ```

  - ```rust
    fn handle_established_outbound_connection(self: &mut Self, connection_id: ConnectionId, peer: PeerId, addr: &libp2p::Multiaddr, role_override: libp2p::core::Endpoint, port_use: libp2p::core::transport::PortUse) -> Result<THandler<Self>, ConnectionDenied> { /* ... */ }
    ```

  - ```rust
    fn on_swarm_event(self: &mut Self, event: FromSwarm<''_>) { /* ... */ }
    ```

  - ```rust
    fn on_connection_handler_event(self: &mut Self, peer_id: PeerId, connection_id: ConnectionId, event: THandlerOutEvent<Self>) { /* ... */ }
    ```

  - ```rust
    fn poll(self: &mut Self, cx: &mut task::Context<''_>) -> task::Poll<ToSwarm<<Self as >::ToSwarm, THandlerInEvent<Self>>> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **RefUnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **UnwindSafe**
- **Sync**
#### Struct `ActorRegistration`

Represents an actor registration in the distributed registry.

Contains the actor's unique ID and its remote type identifier,
which together allow remote peers to locate and communicate
with the actor.

```rust
pub struct ActorRegistration<''a> {
    pub actor_id: crate::actor::ActorId,
    pub remote_id: std::borrow::Cow<''a, str>,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `actor_id` | `crate::actor::ActorId` | The unique identifier of the actor. |
| `remote_id` | `std::borrow::Cow<''a, str>` | The remote type identifier for the actor. |

##### Implementations

###### Methods

- ```rust
  pub fn new(actor_id: ActorId, remote_id: Cow<''a, str>) -> Self { /* ... */ }
  ```
  Creates a new actor registration.

- ```rust
  pub fn into_bytes(self: Self) -> Vec<u8> { /* ... */ }
  ```
  Serializes the actor registration into bytes for storage in the DHT.

- ```rust
  pub fn from_bytes(bytes: &''a [u8]) -> Result<Self, InvalidActorRegistration> { /* ... */ }
  ```
  Deserializes an actor registration from bytes retrieved from the DHT.

- ```rust
  pub fn into_owned(self: Self) -> ActorRegistration<''static> { /* ... */ }
  ```
  Converts a borrowed actor registration into an owned one.

###### Trait Implementations

- **Send**
- **Freeze**
- **Sync**
- **IntoEither**
- **RefUnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **WithSubscriber**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Instrument**
- **Unpin**
- **ReplyError**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **ErasedDestructor**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **AsTaggedImplicit**
- **UnwindSafe**
- **AsTaggedExplicit**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Same**
#### Enum `InvalidActorRegistration`

Errors that can occur when deserializing an actor registration.

```rust
pub enum InvalidActorRegistration {
    EmptyActorRegistration,
    ActorId(crate::actor::ActorIdFromBytesError),
    InvalidRemoteIDUtf8(str::Utf8Error),
}
```

##### Variants

###### `EmptyActorRegistration`

The registration data is empty.

###### `ActorId`

Failed to parse the actor ID from bytes.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `crate::actor::ActorIdFromBytesError` |  |

###### `InvalidRemoteIDUtf8`

The remote ID contains invalid UTF-8.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `str::Utf8Error` |  |

##### Implementations

###### Trait Implementations

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Freeze**
- **RefUnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(err: crate::remote::registry::InvalidActorRegistration) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(err: ActorIdFromBytesError) -> Self { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **WithSubscriber**
- **AsTaggedImplicit**
- **ErasedDestructor**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **ReplyError**
- **Same**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Sync**
- **Send**
- **UnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **IntoEither**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Unpin**
- **AsTaggedExplicit**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

### Traits

#### Trait `RemoteActor`

`RemoteActor` is a trait for identifying actors remotely.

Each remote actor must implement this trait and provide a unique identifier string (`REMOTE_ID`).
The identifier is essential to distinguish between different actor types during remote communication.

## Example with Derive

```
use kameo::{Actor, RemoteActor};

#[derive(Actor, RemoteActor)]
pub struct MyActor;
```

## Example Manual Implementation

```
use kameo::remote::RemoteActor;

pub struct MyActor;

impl RemoteActor for MyActor {
    const REMOTE_ID: &'static str = "my_actor_id";
}
```

```rust
pub trait RemoteActor {
    /* Associated items */
}
```

> This trait is not object-safe and cannot be used in dynamic trait objects.

##### Required Items

###### Associated Constants

- `REMOTE_ID`: The remote identifier string.

#### Trait `RemoteMessage`

`RemoteMessage` is a trait for identifying messages that are sent between remote actors.

Each remote message type must implement this trait and provide a unique identifier string (`REMOTE_ID`).
The unique ID ensures that each message type is recognized correctly during message passing between nodes.

This trait is typically implemented automatically with the [`#[remote_message]`](crate::remote_message) macro.

```rust
pub trait RemoteMessage<M> {
    /* Associated items */
}
```

> This trait is not object-safe and cannot be used in dynamic trait objects.

##### Required Items

###### Associated Constants

- `REMOTE_ID`: The remote identifier string.

### Functions

#### Function `bootstrap`

Bootstrap a simple actor swarm with mDNS discovery for local development.

This convenience function creates and runs a libp2p swarm with:
- TCP and QUIC transports  
- mDNS peer discovery (local network only)
- Automatic listening on an OS-assigned port

For production use or custom configuration, use `kameo::remote::Behaviour`
with your own libp2p swarm setup.

# Example
```ignore
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // One line to get started!
    remote::bootstrap()?;
     
    // Now use remote actors normally
    let actor_ref = MyActor::spawn_default();
    actor_ref.register("my_actor").await?;
    Ok(())
}
```

```rust
pub fn bootstrap() -> Result<libp2p::PeerId, Box<dyn error::Error>> { /* ... */ }
```

#### Function `bootstrap_on`

Bootstrap with a specific listen address.

```rust
pub fn bootstrap_on(addr: &str) -> Result<libp2p::PeerId, Box<dyn error::Error>> { /* ... */ }
```

#### Function `unregister`

Unregisters an actor within the swarm.

This will only unregister an actor previously registered by the current node.

```rust
pub async fn unregister</* synthetic */ impl Into<String>: Into<String>>(name: impl Into<String>) -> Result<(), crate::error::RegistryError> { /* ... */ }
```

### Re-exports

#### Re-export `behaviour::*`

```rust
pub use behaviour::*;
```

#### Re-export `swarm::*`

```rust
pub use swarm::*;
```

## Module `reply`

Constructs for handling replies and errors in Kameo's actor communication.

This module provides the [`Reply`] trait and associated structures for managing message replies within the actor
system. It enables actors to communicate effectively, handling both successful outcomes and errors through a
unified interface.

**Reply Trait Overview**

The `Reply` trait plays a crucial role in Kameo by defining how actors respond to messages.
It is implemented for a variety of common types, facilitating easy adoption and use.
Special attention is given to the `Result` and [`DelegatedReply`] types:
- Implementations for `Result` allow errors returned by actor handlers to be communicated back as
  [`SendError::HandlerError`], integrating closely with Rust’s error handling patterns.
- The `DelegatedReply` type signifies that the actual reply will be managed by another part of the system,
  supporting asynchronous and decoupled communication workflows.
- Importantly, when messages are sent asynchronously with [`tell`](crate::actor::ActorRef::tell) and an error is returned by the actor
  without a direct means for the caller to handle it (due to the absence of a reply expectation), the error is treated
  as a panic within the actor. This behavior will trigger the actor's [`on_panic`](crate::actor::Actor::on_panic) hook, which may result in the actor
  being restarted or stopped based on the [Actor] implementation (which stops the actor by default).

The `Reply` trait, by encompassing a broad range of types and defining specific behaviors for error handling,
ensures that actors can manage their communication responsibilities efficiently and effectively.

```rust
pub mod reply { /* ... */ }
```

### Types

#### Type Alias `BoxReplySender`

A boxed reply sender which will be downcast to the correct type when receiving a reply.

This is reserved for advanced use cases, and misuse of this can result in panics.

```rust
pub type BoxReplySender = oneshot::Sender<Result<crate::message::BoxReply, crate::error::BoxSendError>>;
```

#### Struct `ReplySender`

**Attributes:**

- `#[must_use = "the receiver expects a reply to be sent"]`

A mechanism for sending replies back to the original requester in a message exchange.

`ReplySender` encapsulates the functionality to send a response back to wherever
a request was initiated. It is typically used in scenarios where the
processing of a request is delegated to another actor within the system.
Upon completion of the request handling, `ReplySender` is used to send the result back,
ensuring that the flow of communication is maintained and the requester receives the
necessary response.

This type is designed to be used once per message received; it consumes itself upon sending
a reply to enforce a single use and prevent multiple replies to a single message.

# Usage

A `ReplySender` is obtained as part of the delegation process when handling a message. It should
be used to send a reply once the requested data is available or the operation is complete.

The `ReplySender` provides a clear and straightforward interface for completing the message handling cycle,
facilitating efficient and organized communication within the system.

```rust
pub struct ReplySender<R: ?Sized> {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Methods

- ```rust
  pub fn boxed(self: Self) -> BoxReplySender { /* ... */ }
  ```
  Converts the reply sender to a generic `BoxReplySender`.

- ```rust
  pub fn send(self: Self, reply: R)
where
    R: Reply { /* ... */ }
  ```
  Sends a reply using the current `ReplySender`.

###### Trait Implementations

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<Self, $crate::error::Infallible> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **WithSubscriber**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Instrument**
- **RefUnwindSafe**
- **Freeze**
- **Sync**
- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **UnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **AsTaggedImplicit**
- **Same**
- **Send**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ReplyError**
- **ErasedDestructor**
- **IntoEither**
- **AsTaggedExplicit**
#### Struct `DelegatedReply`

**Attributes:**

- `#[must_use = "the deligated reply should be returned by the handler"]`

A marker type indicating that the reply to a message will be handled elsewhere.

This structure is created by the [`reply_sender`] method on [`Context`].

[`reply_sender`]: method@crate::message::Context::reply_sender
[`Context`]: struct@crate::message::Context

```rust
pub struct DelegatedReply<R> {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Trait Implementations

- **Clone**
  - ```rust
    fn clone(self: &Self) -> DelegatedReply<R> { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Instrument**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **AsTaggedExplicit**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<<Self as >::Ok, <Self as >::Error> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **UnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **AsTaggedImplicit**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Send**
- **ReplyError**
- **RefUnwindSafe**
- **WithSubscriber**
- **Same**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Copy**
- **ErasedDestructor**
- **IntoEither**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Unpin**
#### Struct `ForwardedReply`

A delegated reply that has been forwarded to another actor.

```rust
pub struct ForwardedReply<M, R> {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Trait Implementations

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **IntoEither**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Send**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **WithSubscriber**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **ReplyError**
- **Reply**
  - ```rust
    fn to_result(self: Self) -> Result<<Self as >::Ok, <Self as >::Error> { /* ... */ }
    ```

  - ```rust
    fn into_any_err(self: Self) -> Option<Box<dyn ReplyError>> { /* ... */ }
    ```

  - ```rust
    fn into_value(self: Self) -> <Self as >::Value { /* ... */ }
    ```

  - ```rust
    fn downcast_ok(ok: Box<dyn any::Any>) -> <Self as >::Ok { /* ... */ }
    ```
    If the forwarded reply succeeded, the we can safely assume

  - ```rust
    fn downcast_err<N: ''static>(err: BoxSendError) -> SendError<N, <Self as >::Error> { /* ... */ }
    ```
    The error is either from the inner `R`, or our outer `SendError`.

- **Unpin**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **UnwindSafe**
- **Instrument**
- **ErasedDestructor**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Same**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **AsTaggedExplicit**
- **AsTaggedImplicit**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

### Traits

#### Trait `Reply`

A reply value.

If an Err is returned by a handler, and is unhandled by the caller (ie, the message was sent asynchronously with `tell`),
then the error is treated as a panic in the actor.

This is implemented for all std lib types, and can be implemented on custom types manually or with the derive
macro.

# Example

```
use kameo::Reply;

#[derive(Reply)]
pub struct Foo { }
```

```rust
pub trait Reply: Send + ''static {
    /* Associated items */
}
```

> This trait is not object-safe and cannot be used in dynamic trait objects.

##### Required Items

###### Associated Types

- `Ok`: The success type in the reply.
- `Error`: The error type in the reply.
- `Value`: The type sent back to the receiver.

###### Required Methods

- `to_result`: Converts a reply to a `Result`.
- `into_any_err`: Converts the reply into a `Box<any::Any + Send>` if it's an Err, otherwise `None`.
- `into_value`: Converts the type to Self::Reply.

##### Provided Methods

- ```rust
  fn downcast_ok(ok: Box<dyn any::Any>) -> <Self as >::Ok { /* ... */ }
  ```
  Downcasts a `Box<dyn Any>` into the `Self::Ok` type.

- ```rust
  fn downcast_err<M: ''static>(err: BoxSendError) -> SendError<M, <Self as >::Error> { /* ... */ }
  ```
  Downcasts a `Box<dyn Any>` into a `Self::Error` type.

##### Implementations

This trait is implemented for the following types:

- `DelegatedReply<R>` with <R>
- `ForwardedReply<M, R>` with <M, R>
- `Result<T, E>` with <T, E>
- `crate::actor::ActorId`
- `crate::actor::ActorRef<A>` with <A: Actor>
- `crate::actor::PreparedActor<A>` with <A: Actor>
- `crate::actor::Recipient<M>` with <M: Send>
- `crate::actor::ReplyRecipient<M, Ok, Err>` with <M: Send, Ok: Send, Err: ReplyError>
- `crate::actor::WeakActorRef<A>` with <A: Actor>
- `crate::actor::WeakRecipient<M>` with <M: Send>
- `crate::actor::WeakReplyRecipient<M, Ok, Err>` with <M: Send, Ok: Send, Err: ReplyError>
- `crate::error::ActorStopReason`
- `crate::error::PanicError`
- `crate::error::SendError`
- `crate::mailbox::MailboxReceiver<A>` with <A: Actor>
- `crate::mailbox::MailboxSender<A>` with <A: Actor>
- `crate::message::Context<A, R>` with <A: Actor, R: Reply>
- `ReplySender<R>` with <R: Reply>
- `crate::error::Infallible`
- `()`
- `usize`
- `u8`
- `u16`
- `u32`
- `u64`
- `u128`
- `isize`
- `i8`
- `i16`
- `i32`
- `i64`
- `i128`
- `f32`
- `f64`
- `char`
- `bool`
- `&''static str`
- `String`
- `&''static std::path::Path`
- `std::path::PathBuf`
- `Option<T>` with <T: ''static + Send>
- `std::borrow::Cow<''static, T>` with <T: Clone + Send + Sync>
- `std::sync::Arc<T>` with <T: ''static + Send + Sync>
- `std::sync::Mutex<T>` with <T: ''static + Send>
- `std::sync::RwLock<T>` with <T: ''static + Send>
- `&''static [T; N]` with <const N: usize, T: ''static + Send + Sync>
- `[T; N]` with <const N: usize, T: ''static + Send>
- `&''static [T]` with <T: ''static + Send + Sync>
- `&''static mut T` with <T: ''static + Send>
- `Box<T>` with <T: ''static + Send>
- `Vec<T>` with <T: ''static + Send>
- `std::collections::VecDeque<T>` with <T: ''static + Send>
- `std::collections::LinkedList<T>` with <T: ''static + Send>
- `std::collections::HashMap<K, V>` with <K: ''static + Send, V: ''static + Send>
- `std::collections::BTreeMap<K, V>` with <K: ''static + Send, V: ''static + Send>
- `std::collections::HashSet<T>` with <T: ''static + Send>
- `std::collections::BTreeSet<T>` with <T: ''static + Send>
- `std::collections::BinaryHeap<T>` with <T: ''static + Send>
- `std::num::NonZeroI8`
- `std::num::NonZeroI16`
- `std::num::NonZeroI32`
- `std::num::NonZeroI64`
- `std::num::NonZeroI128`
- `std::num::NonZeroIsize`
- `std::num::NonZeroU8`
- `std::num::NonZeroU16`
- `std::num::NonZeroU32`
- `std::num::NonZeroU64`
- `std::num::NonZeroU128`
- `std::num::NonZeroUsize`
- `std::sync::atomic::AtomicBool`
- `std::sync::atomic::AtomicIsize`
- `std::sync::atomic::AtomicPtr<T>` with <T: ''static + Send>
- `std::sync::atomic::AtomicUsize`
- `std::sync::Once`
- `std::thread::Thread`
- `std::cell::OnceCell<T>` with <T: ''static + Send>
- `std::sync::mpsc::Sender<T>` with <T: ''static + Send>
- `std::sync::mpsc::Receiver<T>` with <T: ''static + Send>
- `futures::stream::FuturesOrdered<T>` with <T: ''static + Send + Future<Output = O>, O: Send>
- `futures::stream::FuturesUnordered<T>` with <T: ''static + Send>
- `tokio::sync::OnceCell<T>` with <T: ''static + Send>
- `tokio::sync::Semaphore`
- `tokio::sync::Notify`
- `tokio::sync::mpsc::Sender<T>` with <T: ''static + Send>
- `tokio::sync::mpsc::Receiver<T>` with <T: ''static + Send>
- `tokio::sync::mpsc::UnboundedSender<T>` with <T: ''static + Send>
- `tokio::sync::mpsc::UnboundedReceiver<T>` with <T: ''static + Send>
- `tokio::sync::watch::Sender<T>` with <T: ''static + Send + Sync>
- `tokio::sync::watch::Receiver<T>` with <T: ''static + Send + Sync>
- `tokio::sync::broadcast::Sender<T>` with <T: ''static + Send>
- `tokio::sync::broadcast::Receiver<T>` with <T: ''static + Send>
- `tokio::sync::oneshot::Sender<T>` with <T: ''static + Send>
- `tokio::sync::oneshot::Receiver<T>` with <T: ''static + Send>
- `tokio::sync::Mutex<T>` with <T: ''static + Send>
- `tokio::sync::RwLock<T>` with <T: ''static + Send>
- `(A)` with <A: ''static + Send>
- `(A, B)` with <A: ''static + Send, B: ''static + Send>
- `(A, B, C)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send>
- `(A, B, C, D)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send>
- `(A, B, C, D, E)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send>
- `(A, B, C, D, E, F)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send>
- `(A, B, C, D, E, F, G)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send>
- `(A, B, C, D, E, F, G, H)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send>
- `(A, B, C, D, E, F, G, H, I)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send, V: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send, V: ''static + Send, W: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send, V: ''static + Send, W: ''static + Send, X: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send, V: ''static + Send, W: ''static + Send, X: ''static + Send, Y: ''static + Send>
- `(A, B, C, D, E, F, G, H, I, J, K, L, M, N, O, P, Q, R, S, T, U, V, W, X, Y, Z)` with <A: ''static + Send, B: ''static + Send, C: ''static + Send, D: ''static + Send, E: ''static + Send, F: ''static + Send, G: ''static + Send, H: ''static + Send, I: ''static + Send, J: ''static + Send, K: ''static + Send, L: ''static + Send, M: ''static + Send, N: ''static + Send, O: ''static + Send, P: ''static + Send, Q: ''static + Send, R: ''static + Send, S: ''static + Send, T: ''static + Send, U: ''static + Send, V: ''static + Send, W: ''static + Send, X: ''static + Send, Y: ''static + Send, Z: ''static + Send>
- `crate::actor::RemoteActorRef<A>` with <A: Actor + crate::remote::RemoteActor>
- `crate::error::RemoteSendError<E>` with <E: ''static + Send>
- `std::sync::atomic::AtomicI8`
- `std::sync::atomic::AtomicU8`
- `std::sync::atomic::AtomicI16`
- `std::sync::atomic::AtomicU16`
- `std::sync::atomic::AtomicI32`
- `std::sync::atomic::AtomicU32`
- `std::sync::atomic::AtomicI64`
- `std::sync::atomic::AtomicU64`

#### Trait `ReplyError`

An error type which can be used in replies.

This is implemented for all types which are `Debug + Send + 'static`.

```rust
pub trait ReplyError: DowncastSend + fmt::Debug + ''static {
    /* Associated items */
}
```

##### Implementations

This trait is implemented for the following types:

- `T` with <T>

## Module `request`

Types for sending requests including messages and queries to actors.

```rust
pub mod request { /* ... */ }
```

### Types

#### Struct `WithoutRequestTimeout`

A type for requests without any timeout set.

```rust
pub struct WithoutRequestTimeout;
```

##### Implementations

###### Trait Implementations

- **UnwindSafe**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> WithoutRequestTimeout { /* ... */ }
    ```

- **Unpin**
- **WithSubscriber**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Copy**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **Sync**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **ErasedDestructor**
- **IntoEither**
- **AsTaggedImplicit**
- **Default**
  - ```rust
    fn default() -> WithoutRequestTimeout { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(_: WithoutRequestTimeout) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(_: WithoutRequestTimeout) -> Self { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Freeze**
- **RefUnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Send**
- **Same**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **AsTaggedExplicit**
- **ReplyError**
#### Struct `WithRequestTimeout`

A type for timeouts in actor requests.

```rust
pub struct WithRequestTimeout(/* private field */);
```

##### Fields

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `private` | *Private field* |

##### Implementations

###### Trait Implementations

- **Unpin**
- **Freeze**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Same**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **AsTaggedExplicit**
- **ErasedDestructor**
- **IntoEither**
- **ReplyError**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Copy**
- **Default**
  - ```rust
    fn default() -> WithRequestTimeout { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **UnwindSafe**
- **AsTaggedImplicit**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Sync**
- **Send**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **WithSubscriber**
- **RefUnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(WithRequestTimeout: WithRequestTimeout) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(WithRequestTimeout: WithRequestTimeout) -> Self { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Instrument**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> WithRequestTimeout { /* ... */ }
    ```

#### Enum `MaybeRequestTimeout`

A type which might contain a request timeout.

This type is used internally for remote messaging and will panic if used incorrectly with any MessageSend trait.

```rust
pub enum MaybeRequestTimeout {
    NoTimeout,
    Timeout(std::time::Duration),
}
```

##### Variants

###### `NoTimeout`

No timeout set.

###### `Timeout`

A timeout with a duration.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::time::Duration` |  |

##### Implementations

###### Trait Implementations

- **Same**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **ReplyError**
- **Unpin**
- **Copy**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(timeout: Option<Duration>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(_: WithoutRequestTimeout) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(WithRequestTimeout: WithRequestTimeout) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(timeout: MaybeRequestTimeout) -> Self { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Freeze**
- **Downcast**
  - ```rust
    fn into_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn into_any_rc(self: Rc<T>) -> Rc<dyn Any> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: &Self) -> &dyn Any + ''static { /* ... */ }
    ```

  - ```rust
    fn as_any_mut(self: &mut Self) -> &mut dyn Any + ''static { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> MaybeRequestTimeout { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Instrument**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **AsTaggedExplicit**
- **Send**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **IntoEither**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **ErasedDestructor**
- **AsTaggedImplicit**
- **UnwindSafe**
### Re-exports

#### Re-export `RemoteAskRequest`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use ask::RemoteAskRequest;
```

#### Re-export `RemoteTellRequest`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use tell::RemoteTellRequest;
```

#### Re-export `AskRequest`

```rust
pub use ask::AskRequest;
```

#### Re-export `BlockingPendingReply`

```rust
pub use ask::BlockingPendingReply;
```

#### Re-export `PendingReply`

```rust
pub use ask::PendingReply;
```

#### Re-export `ReplyRecipientAskRequest`

```rust
pub use ask::ReplyRecipientAskRequest;
```

#### Re-export `RecipientTellRequest`

```rust
pub use tell::RecipientTellRequest;
```

#### Re-export `ReplyRecipientTellRequest`

```rust
pub use tell::ReplyRecipientTellRequest;
```

#### Re-export `TellRequest`

```rust
pub use tell::TellRequest;
```

## Module `prelude`

Commonly used types and functions that can be imported with a single use statement.

```
use kameo::prelude::*;
```

This module includes the most essential actor components, messaging types,
and traits needed for typical actor system usage.

```rust
pub mod prelude { /* ... */ }
```

### Re-exports

#### Re-export `messages`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::messages;
```

#### Re-export `remote_message`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::remote_message;
```

#### Re-export `Actor`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::Actor;
```

#### Re-export `RemoteActor`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::RemoteActor;
```

#### Re-export `Reply`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::Reply;
```

#### Re-export `ActorID`

**Attributes:**

- `#[allow(deprecated)]`

```rust
pub use crate::actor::ActorID;
```

#### Re-export `RemoteActorRef`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use crate::actor::RemoteActorRef;
```

#### Re-export `Actor`

```rust
pub use crate::actor::Actor;
```

#### Re-export `ActorId`

```rust
pub use crate::actor::ActorId;
```

#### Re-export `ActorRef`

```rust
pub use crate::actor::ActorRef;
```

#### Re-export `PreparedActor`

```rust
pub use crate::actor::PreparedActor;
```

#### Re-export `Recipient`

```rust
pub use crate::actor::Recipient;
```

#### Re-export `ReplyRecipient`

```rust
pub use crate::actor::ReplyRecipient;
```

#### Re-export `WeakActorRef`

```rust
pub use crate::actor::WeakActorRef;
```

#### Re-export `WeakRecipient`

```rust
pub use crate::actor::WeakRecipient;
```

#### Re-export `WeakReplyRecipient`

```rust
pub use crate::actor::WeakReplyRecipient;
```

#### Re-export `RemoteSendError`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use crate::error::RemoteSendError;
```

#### Re-export `ActorStopReason`

```rust
pub use crate::error::ActorStopReason;
```

#### Re-export `PanicError`

```rust
pub use crate::error::PanicError;
```

#### Re-export `SendError`

```rust
pub use crate::error::SendError;
```

#### Re-export `mailbox`

```rust
pub use crate::mailbox;
```

#### Re-export `MailboxReceiver`

```rust
pub use crate::mailbox::MailboxReceiver;
```

#### Re-export `MailboxSender`

```rust
pub use crate::mailbox::MailboxSender;
```

#### Re-export `Context`

```rust
pub use crate::message::Context;
```

#### Re-export `Message`

```rust
pub use crate::message::Message;
```

#### Re-export `remote`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use crate::remote;
```

#### Re-export `RemoteActor`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use crate::remote::RemoteActor;
```

#### Re-export `RemoteMessage`

**Attributes:**

- `#[<cfg>(feature = "remote")]`

```rust
pub use crate::remote::RemoteMessage;
```

#### Re-export `DelegatedReply`

```rust
pub use crate::reply::DelegatedReply;
```

#### Re-export `ForwardedReply`

```rust
pub use crate::reply::ForwardedReply;
```

#### Re-export `Reply`

```rust
pub use crate::reply::Reply;
```

#### Re-export `ReplyError`

```rust
pub use crate::reply::ReplyError;
```

#### Re-export `ReplySender`

```rust
pub use crate::reply::ReplySender;
```

## Re-exports

### Re-export `Actor`

```rust
pub use actor::Actor;
```

### Re-export `messages`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::messages;
```

### Re-export `remote_message`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::remote_message;
```

### Re-export `Actor`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::Actor;
```

### Re-export `RemoteActor`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::RemoteActor;
```

### Re-export `Reply`

**Attributes:**

- `#[<cfg>(feature = "macros")]`

```rust
pub use kameo_macros::Reply;
```

### Re-export `Reply`

```rust
pub use reply::Reply;
```