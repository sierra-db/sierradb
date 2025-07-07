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

- **WithSubscriber**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ReplyError**
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

- **Clone**
  - ```rust
    fn clone(self: &Self) -> SendError<M, E> { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &SendError<M, E>) -> bool { /* ... */ }
    ```

- **Send**
- **Eq**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
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

- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
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

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Unpin**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Copy**
- **StructuralPartialEq**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **UnwindSafe**
- **Error**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
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

##### Implementations

###### Trait Implementations

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Sync**
- **Freeze**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Unpin**
- **ReplyError**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **WithSubscriber**
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

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> ActorStopReason { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
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

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **RefUnwindSafe**
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

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Send**
- **UnwindSafe**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **DeserializeOwned**
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

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
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

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Unpin**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Instrument**
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

- **UnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> PanicError { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<D>(deserializer: D) -> Result<Self, <D as >::Error>
where
    D: serde::Deserializer<''de> { /* ... */ }
    ```

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

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

- **Error**
- **Freeze**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **WithSubscriber**
- **DeserializeOwned**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Send**
- **RefUnwindSafe**
- **ReplyError**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

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

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

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

- **Eq**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Freeze**
- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, _other: &Self) -> Option<cmp::Ordering> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Error**
  - ```rust
    fn description(self: &Self) -> &str { /* ... */ }
    ```

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Ord**
  - ```rust
    fn cmp(self: &Self, _other: &Self) -> cmp::Ordering { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Hash**
  - ```rust
    fn hash<H: Hasher>(self: &Self, _: &mut H) { /* ... */ }
    ```

- **Send**
- **Debug**
  - ```rust
    fn fmt(self: &Self, _: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **UnwindSafe**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **WithSubscriber**
- **Unpin**
- **DeserializeOwned**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, _: &Infallible) -> bool { /* ... */ }
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

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Copy**
- **Display**
  - ```rust
    fn fmt(self: &Self, _: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **ReplyError**
- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Infallible { /* ... */ }
    ```

- **RefUnwindSafe**
- **Instrument**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

#### Enum `RegistryError`

An error that can occur when registering & looking up actors by name.

```rust
pub enum RegistryError {
    BadActorType,
    NameAlreadyRegistered,
}
```

##### Variants

###### `BadActorType`

The remote actor was found given the ID, but was not the correct type.

###### `NameAlreadyRegistered`

An actor has already been registered under the name.

##### Implementations

###### Trait Implementations

- **Sync**
- **Send**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
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

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Instrument**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **UnwindSafe**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **WithSubscriber**
- **Error**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Unpin**
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

- **ReplyError**
- **Freeze**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

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

- **Sync**
- **WithSubscriber**
- **UnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Self { /* ... */ }
    ```

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

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

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

- **RefUnwindSafe**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **ReplyError**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
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

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Unpin**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
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

- **WithSubscriber**
- **Freeze**
- **Unpin**
- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
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

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Self { /* ... */ }
    ```

- **Sync**
- **UnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
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

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Send**
- **ReplyError**
- **RefUnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
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

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Send**
- **UnwindSafe**
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

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Freeze**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
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

- **ReplyError**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

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

- **Instrument**
- **Unpin**
- **RefUnwindSafe**
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

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
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

- **Send**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **RefUnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
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

- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Unpin**
- **Instrument**
- **WithSubscriber**
- **Sync**
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

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **Send**
- **RefUnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
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
- **ReplyError**
- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **Instrument**
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

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamMessage<T, S, F> { /* ... */ }
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

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Freeze**
- **Send**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Sync**
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

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ReplyError**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Instrument**
- **WithSubscriber**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Unpin**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **UnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
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

## Module `registry`

**Attributes:**

- `#[<cfg>(not(feature = "remote"))]`

Local actor registry for registering and looking up actors by arbitrary names.

```rust
pub mod registry { /* ... */ }
```

### Types

#### Struct `ActorRegistry`

A local actor registry storing actor refs by name.

```rust
pub struct ActorRegistry {
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
  pub fn new() -> Self { /* ... */ }
  ```
  Creates a new empty actor registry.

- ```rust
  pub fn with_capacity(capacity: usize) -> Self { /* ... */ }
  ```
  Creates a new empty actor registry with at least the specified capacity.

- ```rust
  pub fn capacity(self: &Self) -> usize { /* ... */ }
  ```
  Returns the number of actor refs that can be held without reallocating.

- ```rust
  pub fn names(self: &Self) -> Keys<''_, Cow<''static, str>, Box<dyn Any + Send>> { /* ... */ }
  ```
  An iterator visiting all registered actor refs in arbitrary order.

- ```rust
  pub fn len(self: &Self) -> usize { /* ... */ }
  ```
  The number of registered actor refs.

- ```rust
  pub fn is_empty(self: &Self) -> bool { /* ... */ }
  ```
  Returns `true` if the registry contains no actor refs.

- ```rust
  pub fn clear(self: &mut Self) { /* ... */ }
  ```
  Clears the registry, removing all actor refs. Keeps the allocated memory for reuse.

- ```rust
  pub fn get<A, Q>(self: &mut Self, name: &Q) -> Result<Option<ActorRef<A>>, RegistryError>
where
    A: Actor,
    Q: Hash + Eq + ?Sized,
    Cow<''static, str>: Borrow<Q> { /* ... */ }
  ```
  Gets an actor ref previously registered for a given actor type.

- ```rust
  pub fn contains_name<Q>(self: &Self, name: &Q) -> bool
where
    Q: Hash + Eq + ?Sized,
    Cow<''static, str>: Borrow<Q> { /* ... */ }
  ```
  Returns `true` if an actor has been registered under a given name.

- ```rust
  pub fn insert<A: Actor, /* synthetic */ impl Into<Cow<'static, str>>: Into<Cow<''static, str>>>(self: &mut Self, name: impl Into<Cow<''static, str>>, actor_ref: ActorRef<A>) -> bool { /* ... */ }
  ```
  Inserts a new actor ref under a given name, which can be used later to be looked up.

- ```rust
  pub fn remove<Q>(self: &mut Self, name: &Q) -> bool
where
    Q: Hash + Eq + ?Sized,
    Cow<''static, str>: Borrow<Q> { /* ... */ }
  ```
  Removes a previously registered actor ref under a given name.

###### Trait Implementations

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Default**
  - ```rust
    fn default() -> Self { /* ... */ }
    ```

- **Send**
- **WithSubscriber**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ReplyError**
- **Instrument**
- **Unpin**
- **Freeze**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Sync**
- **UnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
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

- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

### Constants and Statics

#### Static `ACTOR_REGISTRY`

Global actor registry for local actors.

```rust
pub static ACTOR_REGISTRY: once_cell::sync::Lazy<std::sync::Arc<std::sync::Mutex<ActorRegistry>>> = _;
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

- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **WithSubscriber**
- **RefUnwindSafe**
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

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ReplyError**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Freeze**
- **Sync**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Unpin**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

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

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Send**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

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

- **WithSubscriber**
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

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **UnwindSafe**
- **Sync**
- **ReplyError**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Instrument**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Send**
- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

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

- **Clone**
  - ```rust
    fn clone(self: &Self) -> DelegatedReply<R> { /* ... */ }
    ```

- **Freeze**
- **DynMessage**
  - ```rust
    fn handle_dyn(self: Box<T>, state: &mut A, actor_ref: ActorRef<A>, tx: Option<Sender<Result<Box<dyn Any + Send>, SendError<Box<dyn Any + Send>, Box<dyn Any + Send>>>>>) -> Pin<Box<dyn Future<Output = Result<(), Box<dyn ReplyError>>> + Send + ''_>> { /* ... */ }
    ```

  - ```rust
    fn as_any(self: Box<T>) -> Box<dyn Any> { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

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

- **RefUnwindSafe**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Copy**
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

- **Freeze**
- **ReplyError**
- **Instrument**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Unpin**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

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

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **RefUnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Sync**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
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

- **Send**
- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **WithSubscriber**
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **WithSubscriber**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **ReplyError**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **RefUnwindSafe**
- **Instrument**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
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

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
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

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> WithoutRequestTimeout { /* ... */ }
    ```

- **Copy**
- **Default**
  - ```rust
    fn default() -> WithoutRequestTimeout { /* ... */ }
    ```

- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
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
- **Unpin**
- **Sync**
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

- **Instrument**
- **UnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Sync**
- **Default**
  - ```rust
    fn default() -> WithRequestTimeout { /* ... */ }
    ```

- **Unpin**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

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

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> WithRequestTimeout { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Copy**
- **Freeze**
- **ReplyError**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Send**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **RefUnwindSafe**
- **DowncastSync**
  - ```rust
    fn into_any_sync(self: Box<T>) -> Box<dyn Any + Send + Sync> { /* ... */ }
    ```

  - ```rust
    fn into_any_arc(self: Arc<T>) -> Arc<dyn Any + Send + Sync> { /* ... */ }
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

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **UnwindSafe**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> MaybeRequestTimeout { /* ... */ }
    ```

- **DynClone**
  - ```rust
    fn __clone_box(self: &Self, _: Private) -> *mut () { /* ... */ }
    ```

- **Copy**
- **Sync**
- **Freeze**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **RefUnwindSafe**
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

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Instrument**
- **Unpin**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Send**
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

- **DowncastSend**
  - ```rust
    fn into_any_send(self: Box<T>) -> Box<dyn Any + Send> { /* ... */ }
    ```

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

- **ReplyError**
### Re-exports

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

