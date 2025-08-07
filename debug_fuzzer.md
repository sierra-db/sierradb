# Debugging SierraDB Fuzz Test

The fuzz test now has enhanced debugging capabilities to help you investigate validation errors.

## Debugging Environment Variables

### 1. `FUZZ_DEBUG=1` - Enable detailed input logging
```bash
FUZZ_DEBUG=1 cargo fuzz run commands
```
This will print all the generated commands before execution, helping you understand what inputs are causing issues.

### 2. `FUZZ_PANIC_ON_MODEL_ERROR=1` - Panic on model inconsistencies
```bash
FUZZ_PANIC_ON_MODEL_ERROR=1 cargo fuzz run commands
```
This will cause the fuzzer to panic (and generate a backtrace) when it detects model inconsistencies like the error you found.

### 3. Combine both for maximum debugging
```bash
FUZZ_DEBUG=1 FUZZ_PANIC_ON_MODEL_ERROR=1 cargo fuzz run commands
```

## Debugging Output

When a model inconsistency is detected, you'll see output like:
```
=== DEBUGGING MODEL INCONSISTENCY ===
Error: Model has version 0 for stream '\u{8d0d0}' in partition 416 but database returned none
Init args: InitArgs { segment_size: 8192, total_buckets: 32, ... }
Current command 2: GetStreamVersion { partition_id: Some(416), stream_id: Some(...) }
Model state:
  - Partitions: [416, 123, 456]
  - Streams: {(416, StreamId("test")): 0, ...}
  - Total buckets: 32
Saved failing input to: fuzz_failure_12345.json
```

## Analyzing the Error

The error you found suggests:
1. The model thinks there's a stream with version 0 in partition 416
2. But the database returns `None` for that stream version query
3. This indicates a bug in either:
   - The model's tracking of stream versions
   - The database's stream version retrieval
   - The interaction between append operations and version tracking

## Next Steps

1. **Run with debugging enabled** to get the failing input
2. **Examine the JSON file** to see the exact sequence of commands
3. **Look for patterns** - what commands led to this inconsistent state?
4. **Focus on the specific error**: The mismatch between model and database for stream version queries

## Common Issues to Look For

- Race conditions in stream version updates
- Incorrect handling of empty streams
- Partition-to-bucket mapping issues
- Transaction rollback scenarios not properly reflected in the model