# Crate Documentation

**Version:** 0.0.0

**Format Version:** 45

# Module `sierradb`

## Modules

## Module `bucket`

```rust
pub mod bucket { /* ... */ }
```

### Modules

## Module `event_index`

The file format for an MPHF-based index is defined as follows:
- `[0..4]`     : magic marker: `b"EIDX"`
- `[4..12]`    : number of keys (n) as a `u64`
- `[12..20]`   : length of serialized MPHF (L) as a `u64`
- `[20..20+L]` : serialized MPHF bytes (using bincode)
- `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

```rust
pub mod event_index { /* ... */ }
```

### Types

#### Struct `OpenEventIndex`

```rust
pub struct OpenEventIndex {
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
  pub fn create</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> { /* ... */ }
  ```

- ```rust
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> { /* ... */ }
  ```

- ```rust
  pub fn close(self: Self, pool: &ThreadPool) -> Result<ClosedEventIndex, EventIndexError> { /* ... */ }
  ```
  Closes the event index, flushing the index in a background thread.

- ```rust
  pub fn get(self: &Self, event_id: &Uuid) -> Option<u64> { /* ... */ }
  ```

- ```rust
  pub fn insert(self: &mut Self, event_id: Uuid, offset: u64) -> Option<u64> { /* ... */ }
  ```

- ```rust
  pub fn retain<F>(self: &mut Self, f: F)
where
    F: FnMut(&Uuid, &mut u64) -> bool { /* ... */ }
  ```

- ```rust
  pub fn flush(self: &mut Self) -> Result<(Mphf<Uuid>, u64), EventIndexError> { /* ... */ }
  ```

- ```rust
  pub fn hydrate(self: &mut Self, reader: &mut BucketSegmentReader) -> Result<(), EventIndexError> { /* ... */ }
  ```
  Hydrates the index from a reader.

###### Trait Implementations

- **RefUnwindSafe**
- **WithSubscriber**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Instrument**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **IntoEither**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Unpin**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Freeze**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **UnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Is**
- **Send**
#### Struct `ClosedEventIndex`

```rust
pub struct ClosedEventIndex {
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
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, EventIndexError> { /* ... */ }
  ```

- ```rust
  pub fn try_clone(self: &Self) -> Result<Self, EventIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get(self: &mut Self, event_id: &Uuid) -> Result<Option<u64>, EventIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **RefUnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **Send**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Instrument**
- **IntoEither**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Unpin**
- **Sync**
- **UnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Is**
#### Enum `ClosedIndex`

```rust
pub enum ClosedIndex {
    Cache(std::collections::BTreeMap<uuid::Uuid, u64>),
    Mphf {
        mphf: boomphf::Mphf<uuid::Uuid>,
        records_offset: u64,
    },
}
```

##### Variants

###### `Cache`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::collections::BTreeMap<uuid::Uuid, u64>` |  |

###### `Mphf`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `mphf` | `boomphf::Mphf<uuid::Uuid>` |  |
| `records_offset` | `u64` |  |

##### Implementations

###### Trait Implementations

- **Send**
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

- **IntoEither**
- **Unpin**
- **UnwindSafe**
- **Is**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Sync**
- **RefUnwindSafe**
- **WithSubscriber**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Instrument**
- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

## Module `partition_index`

The file format for an MPHF-based partition index is defined as follows:
- `[0..4]`     : magic marker: `b"PIDX"`
- `[4..12]`    : number of keys (n) as a `u64`
- `[12..20]`   : length of serialized MPHF (L) as a `u64`
- `[20..20+L]` : serialized MPHF bytes (using bincode)
- `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

```rust
pub mod partition_index { /* ... */ }
```

### Types

#### Struct `PartitionIndexRecord`

```rust
pub struct PartitionIndexRecord<T> {
    pub sequence_min: u64,
    pub sequence_max: u64,
    pub sequence: u64,
    pub offsets: T,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `sequence_min` | `u64` |  |
| `sequence_max` | `u64` |  |
| `sequence` | `u64` |  |
| `offsets` | `T` |  |

##### Implementations

###### Trait Implementations

- **UnwindSafe**
- **Freeze**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
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

- **Sync**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Instrument**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **RefUnwindSafe**
- **WithSubscriber**
- **Unpin**
- **IntoEither**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> PartitionIndexRecord<T> { /* ... */ }
    ```

- **StructuralPartialEq**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &PartitionIndexRecord<T>) -> bool { /* ... */ }
    ```

- **Eq**
- **Send**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Is**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

#### Enum `PartitionOffsets`

```rust
pub enum PartitionOffsets {
    Offsets(Vec<PartitionSequenceOffset>),
    ExternalBucket,
}
```

##### Variants

###### `Offsets`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Vec<PartitionSequenceOffset>` |  |

###### `ExternalBucket`

##### Implementations

###### Trait Implementations

- **UnwindSafe**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
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

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &PartitionOffsets) -> bool { /* ... */ }
    ```

- **Eq**
- **Unpin**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **Freeze**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(offsets: PartitionOffsets) -> Self { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Sync**
- **WithSubscriber**
- **IntoEither**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> PartitionOffsets { /* ... */ }
    ```

- **StructuralPartialEq**
- **Send**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **RefUnwindSafe**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

#### Struct `PartitionSequenceOffset`

```rust
pub struct PartitionSequenceOffset {
    pub sequence: u64,
    pub offset: u64,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `sequence` | `u64` |  |
| `offset` | `u64` |  |

##### Implementations

###### Trait Implementations

- **Clone**
  - ```rust
    fn clone(self: &Self) -> PartitionSequenceOffset { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &PartitionSequenceOffset) -> bool { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Send**
- **Sync**
- **IntoEither**
- **Freeze**
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Instrument**
- **Is**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **StructuralPartialEq**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Eq**
- **UnwindSafe**
- **WithSubscriber**
- **RefUnwindSafe**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Unpin**
- **Copy**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

#### Enum `ClosedOffsetKind`

```rust
pub enum ClosedOffsetKind {
    Pointer(u64, u32),
    Cached(Vec<PartitionSequenceOffset>),
    ExternalBucket,
}
```

##### Variants

###### `Pointer`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u64` |  |
| 1 | `u32` |  |

###### `Cached`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Vec<PartitionSequenceOffset>` |  |

###### `ExternalBucket`

##### Implementations

###### Trait Implementations

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Sync**
- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **RefUnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **WithSubscriber**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> ClosedOffsetKind { /* ... */ }
    ```

- **Eq**
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

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(offsets: PartitionOffsets) -> Self { /* ... */ }
    ```

- **Is**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &ClosedOffsetKind) -> bool { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Freeze**
- **Instrument**
- **Unpin**
- **StructuralPartialEq**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Send**
#### Struct `OpenPartitionIndex`

```rust
pub struct OpenPartitionIndex {
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
  pub fn create</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn close(self: Self, pool: &ThreadPool) -> Result<ClosedPartitionIndex, PartitionIndexError> { /* ... */ }
  ```
  Closes the partition index, flushing the index in a background thread.

- ```rust
  pub fn get(self: &Self, partition_id: PartitionId) -> Option<&PartitionIndexRecord<PartitionOffsets>> { /* ... */ }
  ```

- ```rust
  pub fn insert(self: &mut Self, partition_id: PartitionId, sequence: u64, offset: u64) -> Result<(), PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn insert_external_bucket(self: &mut Self, partition_id: PartitionId) -> Result<(), PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn flush(self: &mut Self) -> Result<(Mphf<PartitionId>, u64), PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn hydrate(self: &mut Self, reader: &mut BucketSegmentReader) -> Result<(), PartitionIndexError> { /* ... */ }
  ```
  Hydrates the index from a reader.

###### Trait Implementations

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Send**
- **Sync**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Unpin**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **WithSubscriber**
- **Is**
- **Instrument**
- **UnwindSafe**
- **Freeze**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

#### Enum `ClosedIndex`

```rust
pub enum ClosedIndex {
    Cache(std::collections::BTreeMap<super::PartitionId, PartitionIndexRecord<PartitionOffsets>>),
    Mphf {
        mphf: boomphf::Mphf<super::PartitionId>,
        records_offset: u64,
    },
}
```

##### Variants

###### `Cache`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::collections::BTreeMap<super::PartitionId, PartitionIndexRecord<PartitionOffsets>>` |  |

###### `Mphf`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `mphf` | `boomphf::Mphf<super::PartitionId>` |  |
| `records_offset` | `u64` |  |

##### Implementations

###### Trait Implementations

- **Unpin**
- **Freeze**
- **Instrument**
- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **UnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Send**
- **WithSubscriber**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **Is**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

#### Struct `ClosedPartitionIndex`

```rust
pub struct ClosedPartitionIndex {
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
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>) -> Result<Self, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn try_clone(self: &Self) -> Result<Self, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get_key(self: &mut Self, partition_id: PartitionId) -> Result<Option<PartitionIndexRecord<ClosedOffsetKind>>, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get_from_key(self: &Self, key: PartitionIndexRecord<ClosedOffsetKind>) -> Result<PartitionOffsets, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get(self: &mut Self, partition_id: PartitionId) -> Result<Option<PartitionOffsets>, PartitionIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **RefUnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **IntoEither**
- **Instrument**
- **Is**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Unpin**
- **Send**
- **Freeze**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **WithSubscriber**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

#### Struct `PartitionEventIter`

```rust
pub struct PartitionEventIter {
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
  pub async fn next(self: &mut Self, header_only: bool) -> Result<Option<EventRecord>, PartitionIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **Send**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Sync**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **WithSubscriber**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **UnwindSafe**
- **RefUnwindSafe**
- **Freeze**
- **Unpin**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

## Module `segment`

```rust
pub mod segment { /* ... */ }
```

### Types

#### Struct `BucketSegmentHeader`

```rust
pub struct BucketSegmentHeader {
    // Some fields omitted
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

##### Implementations

###### Trait Implementations

- **Eq**
- **RefUnwindSafe**
- **StructuralPartialEq**
- **Copy**
- **IntoEither**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **Send**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> BucketSegmentHeader { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Is**
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

- **Freeze**
- **Sync**
- **UnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Instrument**
- **WithSubscriber**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &BucketSegmentHeader) -> bool { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

#### Struct `FlushedOffset`

```rust
pub struct FlushedOffset(/* private field */);
```

##### Fields

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `private` | *Private field* |

##### Implementations

###### Methods

- ```rust
  pub fn new(offset: Arc<AtomicU64>) -> Self { /* ... */ }
  ```

- ```rust
  pub fn load(self: &Self) -> u64 { /* ... */ }
  ```

###### Trait Implementations

- **Unpin**
- **Freeze**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Send**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> FlushedOffset { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **IntoEither**
- **Instrument**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **WithSubscriber**
- **UnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

### Constants and Statics

#### Constant `SEGMENT_HEADER_SIZE`

```rust
pub const SEGMENT_HEADER_SIZE: usize = _;
```

#### Constant `RECORD_HEADER_SIZE`

```rust
pub const RECORD_HEADER_SIZE: usize = _;
```

#### Constant `EVENT_HEADER_SIZE`

```rust
pub const EVENT_HEADER_SIZE: usize = _;
```

#### Constant `COMMIT_SIZE`

```rust
pub const COMMIT_SIZE: usize = _;
```

### Re-exports

#### Re-export `BucketSegmentIter`

```rust
pub use self::reader::BucketSegmentIter;
```

#### Re-export `BucketSegmentReader`

```rust
pub use self::reader::BucketSegmentReader;
```

#### Re-export `CommitRecord`

```rust
pub use self::reader::CommitRecord;
```

#### Re-export `CommittedEvents`

```rust
pub use self::reader::CommittedEvents;
```

#### Re-export `CommittedEventsIntoIter`

```rust
pub use self::reader::CommittedEventsIntoIter;
```

#### Re-export `EventRecord`

```rust
pub use self::reader::EventRecord;
```

#### Re-export `Record`

```rust
pub use self::reader::Record;
```

#### Re-export `AppendEvent`

```rust
pub use self::writer::AppendEvent;
```

#### Re-export `BucketSegmentWriter`

```rust
pub use self::writer::BucketSegmentWriter;
```

## Module `stream_index`

The file format for an MPHF-based stream index is defined as follows:
- `[0..4]`     : magic marker: `b"SIDX"`
- `[4..12]`    : number of keys (n) as a `u64`
- `[12..20]`   : length of serialized MPHF (L) as a `u64`
- `[20..20+L]` : serialized MPHF bytes (using bincode)
- `[20+L..]`   : records array, exactly n records of RECORD_SIZE bytes each.

```rust
pub mod stream_index { /* ... */ }
```

### Types

#### Struct `StreamIndexRecord`

```rust
pub struct StreamIndexRecord<T> {
    pub partition_key: uuid::Uuid,
    pub version_min: u64,
    pub version_max: u64,
    pub offsets: T,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `partition_key` | `uuid::Uuid` |  |
| `version_min` | `u64` |  |
| `version_max` | `u64` |  |
| `offsets` | `T` |  |

##### Implementations

###### Trait Implementations

- **IntoEither**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **StructuralPartialEq**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

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

- **Eq**
- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Unpin**
- **Sync**
- **Send**
- **WithSubscriber**
- **Is**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &StreamIndexRecord<T>) -> bool { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamIndexRecord<T> { /* ... */ }
    ```

- **Instrument**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **UnwindSafe**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Freeze**
#### Enum `StreamOffsets`

```rust
pub enum StreamOffsets {
    Offsets(Vec<u64>),
    ExternalBucket,
}
```

##### Variants

###### `Offsets`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Vec<u64>` |  |

###### `ExternalBucket`

##### Implementations

###### Trait Implementations

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **WithSubscriber**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **IntoEither**
- **Freeze**
- **Unpin**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(offsets: StreamOffsets) -> Self { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamOffsets { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Eq**
- **Is**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Sync**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
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

- **StructuralPartialEq**
- **UnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &StreamOffsets) -> bool { /* ... */ }
    ```

#### Enum `ClosedOffsetKind`

```rust
pub enum ClosedOffsetKind {
    Pointer(u64, u32),
    Cached(Vec<u64>),
    ExternalBucket,
}
```

##### Variants

###### `Pointer`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u64` |  |
| 1 | `u32` |  |

###### `Cached`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Vec<u64>` |  |

###### `ExternalBucket`

##### Implementations

###### Trait Implementations

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Clone**
  - ```rust
    fn clone(self: &Self) -> ClosedOffsetKind { /* ... */ }
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

- **Freeze**
- **Instrument**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Send**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **StructuralPartialEq**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &ClosedOffsetKind) -> bool { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Unpin**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(offsets: StreamOffsets) -> Self { /* ... */ }
    ```

- **RefUnwindSafe**
- **Eq**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **WithSubscriber**
- **UnwindSafe**
#### Struct `OpenStreamIndex`

```rust
pub struct OpenStreamIndex {
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
  pub fn create</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>, segment_size: usize) -> Result<Self, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>, segment_size: usize) -> Result<Self, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn close(self: Self, pool: &ThreadPool) -> Result<ClosedStreamIndex, StreamIndexError> { /* ... */ }
  ```
  Closes the stream index, flushing the index in a background thread.

- ```rust
  pub fn get(self: &Self, stream_id: &str) -> Option<&StreamIndexRecord<StreamOffsets>> { /* ... */ }
  ```

- ```rust
  pub fn insert(self: &mut Self, stream_id: StreamId, partition_key: Uuid, stream_version: u64, offset: u64) -> Result<(), StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn insert_external_bucket(self: &mut Self, stream_id: StreamId, partition_key: Uuid) -> Result<(), StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn flush(self: &mut Self) -> Result<(Mphf<StreamId>, u64), StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn hydrate(self: &mut Self, reader: &mut BucketSegmentReader) -> Result<(), StreamIndexError> { /* ... */ }
  ```
  Hydrates the index from a reader.

###### Trait Implementations

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

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

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Instrument**
- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Send**
- **UnwindSafe**
- **IntoEither**
- **WithSubscriber**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **Freeze**
#### Enum `ClosedIndex`

```rust
pub enum ClosedIndex {
    Cache(std::collections::BTreeMap<crate::StreamId, StreamIndexRecord<StreamOffsets>>),
    Mphf {
        mphf: boomphf::Mphf<crate::StreamId>,
        records_offset: u64,
    },
}
```

##### Variants

###### `Cache`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::collections::BTreeMap<crate::StreamId, StreamIndexRecord<StreamOffsets>>` |  |

###### `Mphf`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `mphf` | `boomphf::Mphf<crate::StreamId>` |  |
| `records_offset` | `u64` |  |

##### Implementations

###### Trait Implementations

- **UnwindSafe**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Is**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Send**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **Unpin**
- **WithSubscriber**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
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

- **Freeze**
- **Sync**
#### Struct `ClosedStreamIndex`

```rust
pub struct ClosedStreamIndex {
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
  pub fn open</* synthetic */ impl AsRef<Path>: AsRef<Path>>(id: BucketSegmentId, path: impl AsRef<Path>, _segment_size: usize) -> Result<Self, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn try_clone(self: &Self) -> Result<Self, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get_key(self: &mut Self, stream_id: &str) -> Result<Option<StreamIndexRecord<ClosedOffsetKind>>, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get_from_key(self: &Self, key: StreamIndexRecord<ClosedOffsetKind>) -> Result<StreamOffsets, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub fn get(self: &mut Self, stream_id: &str) -> Result<Option<StreamOffsets>, StreamIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Is**
- **Send**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Unpin**
- **UnwindSafe**
- **WithSubscriber**
- **IntoEither**
- **RefUnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Instrument**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Sync**
- **Freeze**
#### Struct `EventStreamIter`

```rust
pub struct EventStreamIter {
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
  pub async fn next(self: &mut Self, header_only: bool) -> Result<Option<EventRecord>, StreamIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Unpin**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **WithSubscriber**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Is**
- **Instrument**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **RefUnwindSafe**
- **UnwindSafe**
- **Send**
- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

### Types

#### Type Alias `BucketId`

```rust
pub type BucketId = u16;
```

#### Type Alias `SegmentId`

```rust
pub type SegmentId = u32;
```

#### Type Alias `PartitionKey`

```rust
pub type PartitionKey = uuid::Uuid;
```

#### Type Alias `PartitionHash`

```rust
pub type PartitionHash = u16;
```

#### Type Alias `PartitionId`

```rust
pub type PartitionId = u16;
```

#### Struct `BucketSegmentId`

```rust
pub struct BucketSegmentId {
    pub bucket_id: BucketId,
    pub segment_id: SegmentId,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `bucket_id` | `BucketId` |  |
| `segment_id` | `SegmentId` |  |

##### Implementations

###### Methods

- ```rust
  pub fn new(bucket_id: BucketId, segment_id: SegmentId) -> Self { /* ... */ }
  ```

- ```rust
  pub fn increment_segment_id(self: &Self) -> Self { /* ... */ }
  ```

###### Trait Implementations

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &BucketSegmentId) -> bool { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **IntoEither**
- **Sync**
- **StructuralPartialEq**
- **Instrument**
- **RefUnwindSafe**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **WithSubscriber**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> BucketSegmentId { /* ... */ }
    ```

- **Eq**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **DeserializeOwned**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Hash**
  - ```rust
    fn hash<__H: $crate::hash::Hasher>(self: &Self, state: &mut __H) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Copy**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Is**
- **Ord**
  - ```rust
    fn cmp(self: &Self, other: &BucketSegmentId) -> $crate::cmp::Ordering { /* ... */ }
    ```

- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, other: &BucketSegmentId) -> $crate::option::Option<$crate::cmp::Ordering> { /* ... */ }
    ```

- **ConstructibleKey**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Freeze**
- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Unpin**
- **UnwindSafe**
- **Send**
#### Enum `SegmentKind`

```rust
pub enum SegmentKind {
    Events,
    EventIndex,
    PartitionIndex,
    StreamIndex,
}
```

##### Variants

###### `Events`

###### `EventIndex`

###### `PartitionIndex`

###### `StreamIndex`

##### Implementations

###### Methods

- ```rust
  pub fn file_name(self: &Self) -> &''static str { /* ... */ }
  ```
  File names for each kind of segment file

- ```rust
  pub fn get_path(self: &Self, base_dir: &Path, bucket_segment_id: BucketSegmentId) -> PathBuf { /* ... */ }
  ```
  Get the full path for a specific segment file

- ```rust
  pub fn parse_path(path: &Path) -> Option<(BucketSegmentId, SegmentKind)> { /* ... */ }
  ```
  Parse a full path back into bucket_segment_id and kind

- ```rust
  pub fn ensure_segment_dir(base_dir: &Path, bucket_segment_id: BucketSegmentId) -> std::io::Result<PathBuf> { /* ... */ }
  ```
  Helper method to create the directory structure for a segment if it

###### Trait Implementations

- **Freeze**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
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

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **WithSubscriber**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
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

- **Sync**
- **Is**
- **Instrument**
- **UnwindSafe**
- **Unpin**
- **IntoEither**
- **FromStr**
  - ```rust
    fn from_str(s: &str) -> Result<Self, <Self as >::Err> { /* ... */ }
    ```

- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Send**
#### Struct `InvalidSegmentKind`

**Attributes:**

- `#[error("invalid segment kind")]`

```rust
pub struct InvalidSegmentKind;
```

##### Implementations

###### Trait Implementations

- **Clone**
  - ```rust
    fn clone(self: &Self) -> InvalidSegmentKind { /* ... */ }
    ```

- **Copy**
- **Error**
- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Unpin**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Default**
  - ```rust
    fn default() -> InvalidSegmentKind { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

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

- **Is**
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Instrument**
- **Send**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Sync**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **UnwindSafe**
- **Freeze**
- **WithSubscriber**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

## Module `database`

```rust
pub mod database { /* ... */ }
```

### Types

#### Struct `Database`

```rust
pub struct Database {
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
  pub fn open</* synthetic */ impl Into<PathBuf>: Into<PathBuf>>(dir: impl Into<PathBuf>) -> Result<Self, DatabaseError> { /* ... */ }
  ```

- ```rust
  pub fn dir(self: &Self) -> &PathBuf { /* ... */ }
  ```

- ```rust
  pub async fn append_events(self: &Self, partition_id: PartitionId, events: Transaction) -> Result<AppendResult, WriteError> { /* ... */ }
  ```

- ```rust
  pub async fn set_confirmations(self: &Self, partition_id: PartitionId, offsets: SmallVec<[u64; 4]>, transaction_id: Uuid, confirmation_count: u8) -> Result<(), WriteError> { /* ... */ }
  ```

- ```rust
  pub async fn read_event(self: &Self, partition_id: PartitionId, event_id: Uuid, header_only: bool) -> Result<Option<EventRecord>, ReadError> { /* ... */ }
  ```

- ```rust
  pub async fn read_transaction(self: &Self, partition_id: PartitionId, first_event_id: Uuid, header_only: bool) -> Result<Option<CommittedEvents>, ReadError> { /* ... */ }
  ```

- ```rust
  pub async fn read_partition(self: &Self, partition_id: PartitionId, from_sequence: u64) -> Result<PartitionEventIter, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub async fn get_partition_sequence(self: &Self, partition_id: PartitionId) -> Result<Option<PartitionLatestSequence>, PartitionIndexError> { /* ... */ }
  ```

- ```rust
  pub async fn read_stream(self: &Self, partition_id: PartitionId, stream_id: StreamId) -> Result<EventStreamIter, StreamIndexError> { /* ... */ }
  ```

- ```rust
  pub async fn get_stream_version(self: &Self, partition_id: PartitionId, stream_id: &Arc<str>) -> Result<Option<StreamLatestVersion>, StreamIndexError> { /* ... */ }
  ```

###### Trait Implementations

- **Instrument**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Send**
- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **RefUnwindSafe**
- **IntoEither**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Unpin**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Is**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Database { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **UnwindSafe**
- **Freeze**
- **WithSubscriber**
#### Struct `DatabaseBuilder`

```rust
pub struct DatabaseBuilder {
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

- ```rust
  pub fn open</* synthetic */ impl Into<PathBuf>: Into<PathBuf>>(self: &Self, dir: impl Into<PathBuf>) -> Result<Database, DatabaseError> { /* ... */ }
  ```

- ```rust
  pub fn segment_size(self: &mut Self, n: usize) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn total_buckets(self: &mut Self, total_buckets: u16) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn bucket_ids</* synthetic */ impl Into<Arc<[BucketId]>>: Into<Arc<[BucketId]>>>(self: &mut Self, bucket_ids: impl Into<Arc<[BucketId]>>) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn bucket_ids_from_range</* synthetic */ impl RangeBounds<BucketId>: RangeBounds<BucketId>>(self: &mut Self, range: impl RangeBounds<BucketId>) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn reader_threads(self: &mut Self, n: u16) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn writer_threads(self: &mut Self, n: u16) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn flush_interval_duration(self: &mut Self, interval: Duration) -> &mut Self { /* ... */ }
  ```

- ```rust
  pub fn flush_interval_events(self: &mut Self, events: u32) -> &mut Self { /* ... */ }
  ```

###### Trait Implementations

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **Send**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
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

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Unpin**
- **Is**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Instrument**
- **Freeze**
- **WithSubscriber**
- **UnwindSafe**
- **Default**
  - ```rust
    fn default() -> Self { /* ... */ }
    ```

- **IntoEither**
#### Enum `ExpectedVersion`

The expected version **before** the event is inserted.

```rust
pub enum ExpectedVersion {
    Any,
    Exists,
    Empty,
    Exact(u64),
}
```

##### Variants

###### `Any`

Accept any version, whether the stream/partition exists or not.

###### `Exists`

The stream/partition must exist (have at least one event).

###### `Empty`

The stream/partition must be empty (have no events yet).

###### `Exact`

The stream/partition must be exactly at this version.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u64` |  |

##### Implementations

###### Methods

- ```rust
  pub fn from_next_version(version: u64) -> Self { /* ... */ }
  ```

- ```rust
  pub fn into_next_version(self: Self) -> Option<u64> { /* ... */ }
  ```

###### Trait Implementations

- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **IntoEither**
- **WithSubscriber**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Default**
  - ```rust
    fn default() -> ExpectedVersion { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> ExpectedVersion { /* ... */ }
    ```

- **Send**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Is**
- **Unpin**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &ExpectedVersion) -> bool { /* ... */ }
    ```

- **Freeze**
- **DeserializeOwned**
- **StructuralPartialEq**
- **Instrument**
- **Eq**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **RefUnwindSafe**
- **Copy**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **UnwindSafe**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

#### Enum `CurrentVersion`

Actual position of a stream.

```rust
pub enum CurrentVersion {
    Empty,
    Current(u64),
}
```

##### Variants

###### `Empty`

The stream/partition doesn't exist.

###### `Current`

The last stream version/partition sequence.

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u64` |  |

##### Implementations

###### Methods

- ```rust
  pub fn next(self: &Self) -> u64 { /* ... */ }
  ```

- ```rust
  pub fn as_expected_version(self: &Self) -> ExpectedVersion { /* ... */ }
  ```

###### Trait Implementations

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **IntoEither**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Send**
- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **UnwindSafe**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Sync**
- **Is**
- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **AddAssign**
  - ```rust
    fn add_assign(self: &mut Self, rhs: u64) { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, other: &CurrentVersion) -> $crate::option::Option<$crate::cmp::Ordering> { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Ord**
  - ```rust
    fn cmp(self: &Self, other: &CurrentVersion) -> $crate::cmp::Ordering { /* ... */ }
    ```

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

- **Eq**
- **RefUnwindSafe**
- **Instrument**
- **StructuralPartialEq**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> CurrentVersion { /* ... */ }
    ```

- **DeserializeOwned**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Unpin**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Copy**
- **WithSubscriber**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &CurrentVersion) -> bool { /* ... */ }
    ```

#### Enum `PartitionLatestSequence`

```rust
pub enum PartitionLatestSequence {
    LatestSequence {
        partition_id: crate::bucket::PartitionId,
        sequence: u64,
    },
    ExternalBucket {
        partition_id: crate::bucket::PartitionId,
    },
}
```

##### Variants

###### `LatestSequence`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `partition_id` | `crate::bucket::PartitionId` |  |
| `sequence` | `u64` |  |

###### `ExternalBucket`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `partition_id` | `crate::bucket::PartitionId` |  |

##### Implementations

###### Trait Implementations

- **UnwindSafe**
- **Sync**
- **RefUnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **Instrument**
- **StructuralPartialEq**
- **Is**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &PartitionLatestSequence) -> bool { /* ... */ }
    ```

- **Send**
- **Freeze**
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

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
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

- **Clone**
  - ```rust
    fn clone(self: &Self) -> PartitionLatestSequence { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Copy**
- **Unpin**
- **IntoEither**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Eq**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

#### Enum `StreamLatestVersion`

```rust
pub enum StreamLatestVersion {
    LatestVersion {
        partition_key: uuid::Uuid,
        version: u64,
    },
    ExternalBucket {
        partition_key: uuid::Uuid,
    },
}
```

##### Variants

###### `LatestVersion`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `partition_key` | `uuid::Uuid` |  |
| `version` | `u64` |  |

###### `ExternalBucket`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `partition_key` | `uuid::Uuid` |  |

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

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Freeze**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Instrument**
- **IntoEither**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamLatestVersion { /* ... */ }
    ```

- **Send**
- **UnwindSafe**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **StructuralPartialEq**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &StreamLatestVersion) -> bool { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Unpin**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Is**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **WithSubscriber**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Copy**
- **Eq**
#### Struct `Transaction`

```rust
pub struct Transaction {
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
  pub fn new(partition_hash: PartitionHash, events: SmallVec<[NewEvent; 4]>) -> Result<Self, EventValidationError> { /* ... */ }
  ```

- ```rust
  pub fn expected_partition_sequence(self: Self, sequence: ExpectedVersion) -> Self { /* ... */ }
  ```

- ```rust
  pub fn partition_key(self: &Self) -> Uuid { /* ... */ }
  ```

- ```rust
  pub fn transaction_id(self: &Self) -> Uuid { /* ... */ }
  ```

- ```rust
  pub fn events(self: &Self) -> &SmallVec<[NewEvent; 4]> { /* ... */ }
  ```

###### Trait Implementations

- **Send**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
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

- **Unpin**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **StructuralPartialEq**
- **Instrument**
- **WithSubscriber**
- **Sync**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **IntoEither**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &Transaction) -> bool { /* ... */ }
    ```

- **Eq**
- **DeserializeOwned**
- **UnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Freeze**
- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **RefUnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> Transaction { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

#### Struct `NewEvent`

```rust
pub struct NewEvent {
    pub event_id: uuid::Uuid,
    pub partition_key: uuid::Uuid,
    pub partition_id: crate::bucket::PartitionId,
    pub stream_id: crate::StreamId,
    pub stream_version: ExpectedVersion,
    pub event_name: String,
    pub timestamp: u64,
    pub metadata: Vec<u8>,
    pub payload: Vec<u8>,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `event_id` | `uuid::Uuid` |  |
| `partition_key` | `uuid::Uuid` |  |
| `partition_id` | `crate::bucket::PartitionId` |  |
| `stream_id` | `crate::StreamId` |  |
| `stream_version` | `ExpectedVersion` |  |
| `event_name` | `String` |  |
| `timestamp` | `u64` |  |
| `metadata` | `Vec<u8>` |  |
| `payload` | `Vec<u8>` |  |

##### Implementations

###### Trait Implementations

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

- **UnwindSafe**
- **Instrument**
- **DeserializeOwned**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

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

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Send**
- **RefUnwindSafe**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Eq**
- **Freeze**
- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &NewEvent) -> bool { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **WithSubscriber**
- **IntoEither**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> NewEvent { /* ... */ }
    ```

- **StructuralPartialEq**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

## Module `error`

```rust
pub mod error { /* ... */ }
```

### Types

#### Enum `ThreadPoolError`

Errors which can occur in background threads.

```rust
pub enum ThreadPoolError {
    FlushEventIndex {
        id: crate::bucket::BucketSegmentId,
        file: std::fs::File,
        index: std::sync::Arc<arc_swap::ArcSwap<event_index::ClosedIndex>>,
        err: EventIndexError,
    },
    FlushPartitionIndex {
        id: crate::bucket::BucketSegmentId,
        file: std::fs::File,
        index: std::sync::Arc<arc_swap::ArcSwap<partition_index::ClosedIndex>>,
        err: PartitionIndexError,
    },
    FlushStreamIndex {
        id: crate::bucket::BucketSegmentId,
        file: std::fs::File,
        index: std::sync::Arc<arc_swap::ArcSwap<stream_index::ClosedIndex>>,
        err: StreamIndexError,
    },
}
```

##### Variants

###### `FlushEventIndex`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `id` | `crate::bucket::BucketSegmentId` |  |
| `file` | `std::fs::File` |  |
| `index` | `std::sync::Arc<arc_swap::ArcSwap<event_index::ClosedIndex>>` |  |
| `err` | `EventIndexError` |  |

###### `FlushPartitionIndex`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `id` | `crate::bucket::BucketSegmentId` |  |
| `file` | `std::fs::File` |  |
| `index` | `std::sync::Arc<arc_swap::ArcSwap<partition_index::ClosedIndex>>` |  |
| `err` | `PartitionIndexError` |  |

###### `FlushStreamIndex`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `id` | `crate::bucket::BucketSegmentId` |  |
| `file` | `std::fs::File` |  |
| `index` | `std::sync::Arc<arc_swap::ArcSwap<stream_index::ClosedIndex>>` |  |
| `err` | `StreamIndexError` |  |

##### Implementations

###### Trait Implementations

- **Sync**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Error**
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

- **Send**
- **Is**
- **UnwindSafe**
- **Unpin**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **IntoEither**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **RefUnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **Freeze**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
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

#### Enum `DatabaseError`

```rust
pub enum DatabaseError {
    Read(ReadError),
    Write(WriteError),
    EventIndex(EventIndexError),
    PartitionIndex(PartitionIndexError),
    StreamIndex(StreamIndexError),
    ThreadPool(rayon::ThreadPoolBuildError),
    Io(io::Error),
}
```

##### Variants

###### `Read`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `ReadError` |  |

###### `Write`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `WriteError` |  |

###### `EventIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `EventIndexError` |  |

###### `PartitionIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `PartitionIndexError` |  |

###### `StreamIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `StreamIndexError` |  |

###### `ThreadPool`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `rayon::ThreadPoolBuildError` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **Send**
- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

- **UnwindSafe**
- **IntoEither**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

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
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: WriteError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: PartitionIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: StreamIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ThreadPoolBuildError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **RefUnwindSafe**
- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Sync**
- **Freeze**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Unpin**
- **Instrument**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

#### Enum `ReadError`

```rust
pub enum ReadError {
    Crc32cMismatch {
        offset: u64,
    },
    ConfirmationCountCrc32cMismatch {
        offset: u64,
    },
    InvalidStreamIdUtf8(std::str::Utf8Error),
    InvalidEventNameUtf8(std::str::Utf8Error),
    UnknownRecordType(u8),
    NoThreadReply,
    Bincode(bincode::error::DecodeError),
    EventIndex(Box<EventIndexError>),
    Io(io::Error),
}
```

##### Variants

###### `Crc32cMismatch`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |

###### `ConfirmationCountCrc32cMismatch`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |

###### `InvalidStreamIdUtf8`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::str::Utf8Error` |  |

###### `InvalidEventNameUtf8`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::str::Utf8Error` |  |

###### `UnknownRecordType`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `u8` |  |

###### `NoThreadReply`

###### `Bincode`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::DecodeError` |  |

###### `EventIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `Box<EventIndexError>` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Sync**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Send**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Is**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

- **Freeze**
- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **IntoEither**
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
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: bincode::error::DecodeError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: Box<EventIndexError>) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

- **UnwindSafe**
- **RefUnwindSafe**
- **Instrument**
- **WithSubscriber**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

#### Enum `WriteError`

```rust
pub enum WriteError {
    BadSystemTime,
    BucketWriterNotFound,
    StreamVersionTooHigh,
    WriterThreadNotRunning,
    EventsExceedSegmentSize,
    OffsetExceedsFileSize {
        offset: u64,
        size: u64,
    },
    WrongExpectedSequence {
        partition_id: crate::bucket::PartitionId,
        current: crate::database::CurrentVersion,
        expected: crate::database::ExpectedVersion,
    },
    WrongExpectedVersion {
        stream_id: crate::StreamId,
        current: crate::database::CurrentVersion,
        expected: crate::database::ExpectedVersion,
    },
    WrongEventId {
        offset: u64,
        found: uuid::Uuid,
        expected: uuid::Uuid,
    },
    WrongTransactionId {
        offset: u64,
        found: uuid::Uuid,
        expected: uuid::Uuid,
    },
    NoThreadReply,
    Bincode(bincode::error::EncodeError),
    Validation(EventValidationError),
    Read(ReadError),
    EventIndex(EventIndexError),
    StreamIndex(StreamIndexError),
    PartitionIndex(PartitionIndexError),
    Io(io::Error),
}
```

##### Variants

###### `BadSystemTime`

###### `BucketWriterNotFound`

###### `StreamVersionTooHigh`

###### `WriterThreadNotRunning`

###### `EventsExceedSegmentSize`

###### `OffsetExceedsFileSize`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |
| `size` | `u64` |  |

###### `WrongExpectedSequence`

Wrong expected version

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `partition_id` | `crate::bucket::PartitionId` |  |
| `current` | `crate::database::CurrentVersion` |  |
| `expected` | `crate::database::ExpectedVersion` |  |

###### `WrongExpectedVersion`

Wrong expected version

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `stream_id` | `crate::StreamId` |  |
| `current` | `crate::database::CurrentVersion` |  |
| `expected` | `crate::database::ExpectedVersion` |  |

###### `WrongEventId`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |
| `found` | `uuid::Uuid` |  |
| `expected` | `uuid::Uuid` |  |

###### `WrongTransactionId`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |
| `found` | `uuid::Uuid` |  |
| `expected` | `uuid::Uuid` |  |

###### `NoThreadReply`

###### `Bincode`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::EncodeError` |  |

###### `Validation`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `EventValidationError` |  |

###### `Read`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `ReadError` |  |

###### `EventIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `EventIndexError` |  |

###### `StreamIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `StreamIndexError` |  |

###### `PartitionIndex`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `PartitionIndexError` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **Unpin**
- **IntoEither**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **RefUnwindSafe**
- **Sync**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
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

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Is**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **UnwindSafe**
- **Send**
- **Freeze**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(source: WriteError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: bincode::error::EncodeError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: StreamIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: PartitionIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(_: SystemTimeError) -> Self { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Instrument**
- **WithSubscriber**
- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

#### Enum `EventIndexError`

```rust
pub enum EventIndexError {
    DeserializeMphf(bincode::error::DecodeError),
    SerializeMphf(bincode::error::EncodeError),
    CorruptHeader,
    CorruptNumSlots,
    CorruptRecord {
        offset: u64,
    },
    Read(ReadError),
    Io(io::Error),
}
```

##### Variants

###### `DeserializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::DecodeError` |  |

###### `SerializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::EncodeError` |  |

###### `CorruptHeader`

###### `CorruptNumSlots`

###### `CorruptRecord`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |

###### `Read`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `ReadError` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Send**
- **UnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **Freeze**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(source: EventIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Sync**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

- **WithSubscriber**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **IntoEither**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Instrument**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Unpin**
- **RefUnwindSafe**
#### Enum `PartitionIndexError`

```rust
pub enum PartitionIndexError {
    Bloom {
        err: &''static str,
    },
    DeserializeMphf(bincode::error::DecodeError),
    SerializeMphf(bincode::error::EncodeError),
    CorruptHeader,
    CorruptNumSlots,
    CorruptLen,
    CorruptRecord {
        offset: u64,
    },
    EventCountOverflow,
    InvalidStreamIdUtf8(std::str::Utf8Error),
    PartitionIdOffsetExists,
    PartitionIdMappedToExternalBucket,
    SegmentNotFound {
        bucket_segment_id: crate::bucket::BucketSegmentId,
    },
    Read(ReadError),
    Validation(EventValidationError),
    Io(io::Error),
}
```

##### Variants

###### `Bloom`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `err` | `&''static str` |  |

###### `DeserializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::DecodeError` |  |

###### `SerializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::EncodeError` |  |

###### `CorruptHeader`

###### `CorruptNumSlots`

###### `CorruptLen`

###### `CorruptRecord`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |

###### `EventCountOverflow`

###### `InvalidStreamIdUtf8`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::str::Utf8Error` |  |

###### `PartitionIdOffsetExists`

###### `PartitionIdMappedToExternalBucket`

###### `SegmentNotFound`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `bucket_segment_id` | `crate::bucket::BucketSegmentId` |  |

###### `Read`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `ReadError` |  |

###### `Validation`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `EventValidationError` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **UnwindSafe**
- **Sync**
- **WithSubscriber**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Send**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Is**
- **Freeze**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **RefUnwindSafe**
- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

- **IntoEither**
- **Instrument**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Unpin**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(source: PartitionIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: PartitionIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

#### Enum `StreamIndexError`

```rust
pub enum StreamIndexError {
    Bloom {
        err: &''static str,
    },
    DeserializeMphf(bincode::error::DecodeError),
    SerializeMphf(bincode::error::EncodeError),
    CorruptHeader,
    CorruptNumSlots,
    CorruptLen,
    CorruptRecord {
        offset: u64,
    },
    InvalidStreamIdUtf8(std::str::Utf8Error),
    StreamIdOffsetExists,
    StreamIdMappedToExternalBucket,
    SegmentNotFound {
        bucket_segment_id: crate::bucket::BucketSegmentId,
    },
    Read(ReadError),
    Validation(EventValidationError),
    Io(io::Error),
}
```

##### Variants

###### `Bloom`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `err` | `&''static str` |  |

###### `DeserializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::DecodeError` |  |

###### `SerializeMphf`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `bincode::error::EncodeError` |  |

###### `CorruptHeader`

###### `CorruptNumSlots`

###### `CorruptLen`

###### `CorruptRecord`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `offset` | `u64` |  |

###### `InvalidStreamIdUtf8`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `std::str::Utf8Error` |  |

###### `StreamIdOffsetExists`

###### `StreamIdMappedToExternalBucket`

###### `SegmentNotFound`

Fields:

| Name | Type | Documentation |
|------|------|---------------|
| `bucket_segment_id` | `crate::bucket::BucketSegmentId` |  |

###### `Read`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `ReadError` |  |

###### `Validation`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `EventValidationError` |  |

###### `Io`

Fields:

| Index | Type | Documentation |
|-------|------|---------------|
| 0 | `io::Error` |  |

##### Implementations

###### Trait Implementations

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **IntoEither**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **RefUnwindSafe**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **UnwindSafe**
- **Freeze**
- **Instrument**
- **Is**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Send**
- **Error**
  - ```rust
    fn source(self: &Self) -> ::core::option::Option<&dyn ::thiserror::__private::Error + ''static> { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(source: StreamIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: StreamIndexError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: ReadError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: io::Error) -> Self { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Unpin**
- **Sync**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
#### Enum `EventValidationError`

```rust
pub enum EventValidationError {
    InvalidEventId,
    PartitionKeyMismatch,
    EmptyTransaction,
}
```

##### Variants

###### `InvalidEventId`

###### `PartitionKeyMismatch`

###### `EmptyTransaction`

##### Implementations

###### Trait Implementations

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

  - ```rust
    fn from(source: EventValidationError) -> Self { /* ... */ }
    ```

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
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

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Unpin**
- **WithSubscriber**
- **Is**
- **Sync**
- **IntoEither**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Error**
- **RefUnwindSafe**
- **Send**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **UnwindSafe**
- **Instrument**
#### Enum `StreamIdError`

```rust
pub enum StreamIdError {
    InvalidLength,
    ContainsNullByte,
}
```

##### Variants

###### `InvalidLength`

###### `ContainsNullByte`

##### Implementations

###### Trait Implementations

- **Sync**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **IntoEither**
- **WithSubscriber**
- **RefUnwindSafe**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Instrument**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Display**
  - ```rust
    fn fmt(self: &Self, __formatter: &mut ::core::fmt::Formatter<''_>) -> ::core::fmt::Result { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Copy**
- **Unpin**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **Error**
- **Freeze**
- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Is**
- **UnwindSafe**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Send**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamIdError { /* ... */ }
    ```

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

## Module `id`

```rust
pub mod id { /* ... */ }
```

### Functions

#### Function `stream_id_partition_id`

Hashes the stream id, and performs a modulo on the lowest 16 bits of the
hash.

```rust
pub fn stream_id_partition_id(stream_id: &str) -> crate::bucket::PartitionId { /* ... */ }
```

#### Function `stream_id_bucket`

```rust
pub fn stream_id_bucket(stream_id: &str, num_buckets: u16) -> crate::bucket::BucketId { /* ... */ }
```

#### Function `uuid_v7_with_partition_hash`

Returns a UUID “inspired” by v7, except that 16 bits from the stream-id hash
are embedded in it (bits 46–61 of the final 128-bit value).

Layout (from MSB to LSB):
- 48 bits: timestamp (ms since Unix epoch)
- 12 bits: random
- 4 bits: version (0x7)
- 2 bits: variant (binary 10)
- 16 bits: stream-id hash (lower 16 bits)
- 46 bits: random

```rust
pub fn uuid_v7_with_partition_hash(partition_hash: crate::bucket::PartitionHash) -> uuid::Uuid { /* ... */ }
```

#### Function `uuid_to_partition_hash`

Extracts the embedded 16-bit hash from a UUID.

```rust
pub fn uuid_to_partition_hash(uuid: uuid::Uuid) -> crate::bucket::PartitionHash { /* ... */ }
```

#### Function `extract_event_id_bucket`

```rust
pub fn extract_event_id_bucket(uuid: uuid::Uuid, num_buckets: u16) -> crate::bucket::BucketId { /* ... */ }
```

#### Function `partition_id_to_bucket`

```rust
pub fn partition_id_to_bucket(partition_id: crate::bucket::PartitionId, num_buckets: u16) -> crate::bucket::BucketId { /* ... */ }
```

#### Function `validate_event_id`

```rust
pub fn validate_event_id(event_id: uuid::Uuid, partition_hash: crate::bucket::PartitionHash) -> bool { /* ... */ }
```

#### Function `set_uuid_flag`

```rust
pub fn set_uuid_flag(uuid: uuid::Uuid, flag: bool) -> uuid::Uuid { /* ... */ }
```

#### Function `get_uuid_flag`

```rust
pub fn get_uuid_flag(uuid: &uuid::Uuid) -> bool { /* ... */ }
```

### Constants and Statics

#### Constant `NAMESPACE_PARTITION_KEY`

```rust
pub const NAMESPACE_PARTITION_KEY: uuid::Uuid = _;
```

## Module `reader_thread_pool`

```rust
pub mod reader_thread_pool { /* ... */ }
```

### Types

#### Struct `ReaderSet`

```rust
pub struct ReaderSet {
    pub reader: crate::bucket::segment::BucketSegmentReader,
    pub event_index: Option<crate::bucket::event_index::ClosedEventIndex>,
    pub partition_index: Option<crate::bucket::partition_index::ClosedPartitionIndex>,
    pub stream_index: Option<crate::bucket::stream_index::ClosedStreamIndex>,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `reader` | `crate::bucket::segment::BucketSegmentReader` |  |
| `event_index` | `Option<crate::bucket::event_index::ClosedEventIndex>` |  |
| `partition_index` | `Option<crate::bucket::partition_index::ClosedPartitionIndex>` |  |
| `stream_index` | `Option<crate::bucket::stream_index::ClosedStreamIndex>` |  |

##### Implementations

###### Trait Implementations

- **RefUnwindSafe**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Send**
- **Is**
- **Unpin**
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

- **Instrument**
- **WithSubscriber**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Sync**
- **UnwindSafe**
- **IntoEither**
- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

#### Struct `ReaderThreadPool`

```rust
pub struct ReaderThreadPool {
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
  pub fn new(num_workers: usize) -> Self { /* ... */ }
  ```
  Spawns threads to process read requests in a thread pool.

- ```rust
  pub fn spawn<OP, IN>(self: &Self, op: OP)
where
    OP: FnOnce(fn(IN)) + Send + ''static,
    IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>) { /* ... */ }
  ```

- ```rust
  pub fn install<OP, R, IN, RR>(self: &Self, op: OP) -> R
where
    OP: FnOnce(fn(IN) -> RR) -> R + Send,
    IN: FnOnce(&mut HashMap<BucketId, BTreeMap<SegmentId, ReaderSet>>) -> RR,
    R: Send { /* ... */ }
  ```

- ```rust
  pub fn add_bucket_segment(self: &Self, bucket_segment_id: BucketSegmentId, reader: &BucketSegmentReader, event_index: Option<&ClosedEventIndex>, partition_index: Option<&ClosedPartitionIndex>, stream_index: Option<&ClosedStreamIndex>) { /* ... */ }
  ```
  Adds a bucket segment reader to all workers in the thread pool.

###### Trait Implementations

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Send**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
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

- **Instrument**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> ReaderThreadPool { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **IntoEither**
- **Freeze**
- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Sync**
- **WithSubscriber**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Is**
- **RefUnwindSafe**
- **UnwindSafe**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

## Module `writer_thread_pool`

```rust
pub mod writer_thread_pool { /* ... */ }
```

### Types

#### Type Alias `LiveIndexes`

```rust
pub type LiveIndexes = std::sync::Arc<tokio::sync::RwLock<LiveIndexSet>>;
```

#### Struct `LiveIndexSet`

```rust
pub struct LiveIndexSet {
    pub event_index: crate::bucket::event_index::OpenEventIndex,
    pub partition_index: crate::bucket::partition_index::OpenPartitionIndex,
    pub stream_index: crate::bucket::stream_index::OpenStreamIndex,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `event_index` | `crate::bucket::event_index::OpenEventIndex` |  |
| `partition_index` | `crate::bucket::partition_index::OpenPartitionIndex` |  |
| `stream_index` | `crate::bucket::stream_index::OpenStreamIndex` |  |

##### Implementations

###### Trait Implementations

- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **WithSubscriber**
- **Freeze**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Unpin**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **Sync**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

- **Instrument**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **IntoEither**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Send**
- **RefUnwindSafe**
- **UnwindSafe**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Is**
#### Struct `WriterThreadPool`

```rust
pub struct WriterThreadPool {
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
  pub fn new</* synthetic */ impl Into<PathBuf>: Into<PathBuf>>(dir: impl Into<PathBuf>, segment_size: usize, bucket_ids: Arc<[BucketId]>, num_threads: u16, flush_interval_duration: Duration, flush_interval_events: u32, reader_pool: &ReaderThreadPool, thread_pool: &Arc<ThreadPool>) -> Result<Self, WriteError> { /* ... */ }
  ```

- ```rust
  pub fn indexes(self: &Self) -> &Arc<HashMap<BucketId, (Arc<AtomicU32>, LiveIndexes)>> { /* ... */ }
  ```

- ```rust
  pub async fn append_events(self: &Self, bucket_id: BucketId, batch: Transaction) -> Result<AppendResult, WriteError> { /* ... */ }
  ```

- ```rust
  pub async fn set_confirmations(self: &Self, bucket_id: BucketId, offsets: SmallVec<[u64; 4]>, transaction_id: Uuid, confirmation_count: u8) -> Result<(), WriteError> { /* ... */ }
  ```
  Marks an event/events as confirmed by `confirmation_count` partitions,

- ```rust
  pub async fn with_event_index<F, R>(self: &Self, bucket_id: BucketId, f: F) -> Option<R>
where
    F: FnOnce(&Arc<AtomicU32>, &OpenEventIndex) -> R { /* ... */ }
  ```

- ```rust
  pub async fn with_partition_index<F, R>(self: &Self, bucket_id: BucketId, f: F) -> Option<R>
where
    F: FnOnce(&Arc<AtomicU32>, &OpenPartitionIndex) -> R { /* ... */ }
  ```

- ```rust
  pub async fn with_stream_index<F, R>(self: &Self, bucket_id: BucketId, f: F) -> Option<R>
where
    F: FnOnce(&Arc<AtomicU32>, &OpenStreamIndex) -> R { /* ... */ }
  ```

###### Trait Implementations

- **Is**
- **RefUnwindSafe**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Freeze**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> WriterThreadPool { /* ... */ }
    ```

- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
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

- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **UnwindSafe**
- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **Unpin**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Send**
- **Sync**
- **Instrument**
- **IntoEither**
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

- **WithSubscriber**
#### Struct `AppendResult`

```rust
pub struct AppendResult {
    pub offsets: smallvec::SmallVec<[u64; 4]>,
    pub partition_sequence: u64,
    pub stream_versions: std::collections::HashMap<crate::StreamId, u64>,
}
```

##### Fields

| Name | Type | Documentation |
|------|------|---------------|
| `offsets` | `smallvec::SmallVec<[u64; 4]>` |  |
| `partition_sequence` | `u64` |  |
| `stream_versions` | `std::collections::HashMap<crate::StreamId, u64>` |  |

##### Implementations

###### Trait Implementations

- **Instrument**
- **WithSubscriber**
- **Sync**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
    ```

- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **UnwindSafe**
- **Freeze**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Send**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

- **Unpin**
- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **CloneToUninit**
  - ```rust
    unsafe fn clone_to_uninit(self: &Self, dest: *mut u8) { /* ... */ }
    ```

- **IntoEither**
- **DeserializeOwned**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **StructuralPartialEq**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &AppendResult) -> bool { /* ... */ }
    ```

- **RefUnwindSafe**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Clone**
  - ```rust
    fn clone(self: &Self) -> AppendResult { /* ... */ }
    ```

- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **Eq**
- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **Is**
- **From**
  - ```rust
    fn from(t: T) -> T { /* ... */ }
    ```
    Returns the argument unchanged.

## Types

### Struct `StreamId`

**Attributes:**

- `#[serde(transparent)]`

```rust
pub struct StreamId {
    // Some fields omitted
}
```

#### Fields

| Name | Type | Documentation |
|------|------|---------------|
| *private fields* | ... | *Some fields have been omitted* |

#### Implementations

##### Methods

- ```rust
  pub fn new</* synthetic */ impl Into<Arc<str>>: Into<Arc<str>>>(s: impl Into<Arc<str>>) -> Result<Self, StreamIdError> { /* ... */ }
  ```

- ```rust
  pub unsafe fn new_unchecked</* synthetic */ impl Into<Arc<str>>: Into<Arc<str>>>(s: impl Into<Arc<str>>) -> Self { /* ... */ }
  ```
  # Safety

##### Trait Implementations

- **TryFrom**
  - ```rust
    fn try_from(value: U) -> Result<T, <T as TryFrom<U>>::Error> { /* ... */ }
    ```

- **Eq**
- **Is**
- **Borrow**
  - ```rust
    fn borrow(self: &Self) -> &T { /* ... */ }
    ```

  - ```rust
    fn borrow(self: &Self) -> &str { /* ... */ }
    ```

- **Instrument**
- **Clone**
  - ```rust
    fn clone(self: &Self) -> StreamId { /* ... */ }
    ```

- **Sync**
- **VZip**
  - ```rust
    fn vzip(self: Self) -> V { /* ... */ }
    ```

- **PartialOrd**
  - ```rust
    fn partial_cmp(self: &Self, other: &StreamId) -> $crate::option::Option<$crate::cmp::Ordering> { /* ... */ }
    ```

- **Ord**
  - ```rust
    fn cmp(self: &Self, other: &StreamId) -> $crate::cmp::Ordering { /* ... */ }
    ```

- **Freeze**
- **AsRef**
  - ```rust
    fn as_ref(self: &Self) -> &str { /* ... */ }
    ```

- **Send**
- **Into**
  - ```rust
    fn into(self: Self) -> U { /* ... */ }
    ```
    Calls `U::from(self)`.

- **BorrowMut**
  - ```rust
    fn borrow_mut(self: &mut Self) -> &mut T { /* ... */ }
    ```

- **Receiver**
- **TryInto**
  - ```rust
    fn try_into(self: Self) -> Result<U, <U as TryFrom<T>>::Error> { /* ... */ }
    ```

- **WithSubscriber**
- **ConstructibleKey**
- **DeserializeOwned**
- **ToOwned**
  - ```rust
    fn to_owned(self: &Self) -> T { /* ... */ }
    ```

  - ```rust
    fn clone_into(self: &Self, target: &mut T) { /* ... */ }
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

- **Display**
  - ```rust
    fn fmt(self: &Self, f: &mut fmt::Formatter<''_>) -> fmt::Result { /* ... */ }
    ```

- **IntoEither**
- **Any**
  - ```rust
    fn type_id(self: &Self) -> TypeId { /* ... */ }
    ```

- **Deref**
  - ```rust
    fn deref(self: &Self) -> &<Self as >::Target { /* ... */ }
    ```

- **Unpin**
- **UnwindSafe**
- **Default**
  - ```rust
    fn default() -> StreamId { /* ... */ }
    ```

- **Hash**
  - ```rust
    fn hash<__H: $crate::hash::Hasher>(self: &Self, state: &mut __H) { /* ... */ }
    ```

- **ToString**
  - ```rust
    fn to_string(self: &Self) -> String { /* ... */ }
    ```

- **Pointable**
  - ```rust
    unsafe fn init(init: <T as Pointable>::Init) -> usize { /* ... */ }
    ```

  - ```rust
    unsafe fn deref<''a>(ptr: usize) -> &''a T { /* ... */ }
    ```

  - ```rust
    unsafe fn deref_mut<''a>(ptr: usize) -> &''a mut T { /* ... */ }
    ```

  - ```rust
    unsafe fn drop(ptr: usize) { /* ... */ }
    ```

- **StructuralPartialEq**
- **Serialize**
  - ```rust
    fn serialize<__S>(self: &Self, __serializer: __S) -> _serde::__private::Result<<__S as >::Ok, <__S as >::Error>
where
    __S: _serde::Serializer { /* ... */ }
    ```

- **RefUnwindSafe**
- **Debug**
  - ```rust
    fn fmt(self: &Self, f: &mut $crate::fmt::Formatter<''_>) -> $crate::fmt::Result { /* ... */ }
    ```

- **Deserialize**
  - ```rust
    fn deserialize<__D>(__deserializer: __D) -> _serde::__private::Result<Self, <__D as >::Error>
where
    __D: _serde::Deserializer<''de> { /* ... */ }
    ```

- **PartialEq**
  - ```rust
    fn eq(self: &Self, other: &StreamId) -> bool { /* ... */ }
    ```

## Constants and Statics

### Constant `STREAM_ID_SIZE`

```rust
pub const STREAM_ID_SIZE: usize = 64;
```

### Constant `MAX_REPLICATION_FACTOR`

```rust
pub const MAX_REPLICATION_FACTOR: usize = 12;
```

## Macros

### Macro `from_bytes`

**Attributes:**

- `#[macro_export]`

```rust
pub macro_rules! from_bytes {
    /* macro_rules! from_bytes {
    ($buf:expr, $pos:expr, [ $( $t:tt ),* ]) => { ... };
    ($buf:expr, $pos:expr, str, $len:expr) => { ... };
    ($buf:expr, $pos:expr, &[u8], $len:expr) => { ... };
    ($buf:expr, $pos:expr, Uuid) => { ... };
    ($buf:expr, $pos:expr, u16) => { ... };
    ($buf:expr, $pos:expr, u32) => { ... };
    ($buf:expr, $pos:expr, u64) => { ... };
    ($buf:expr, $pos:expr, $t:ty=>from_le_bytes) => { ... };
    ($buf:expr, $pos:expr, $len:expr, |$slice:ident| $( $tt:tt )* ) => { ... };
    ($buf:expr, $( $tt:tt )*) => { ... };
} */
}
```

