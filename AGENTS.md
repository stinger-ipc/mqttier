# AGENTS.md: Instructions for AI Agents

This document provides guidance for AI agents working on the `mqttier` project.

## Project Overview

**MQTTier** is a Rust MQTT client library that wraps `rumqttc`, providing:
- Async/await support via tokio
- JSON serialization with serde
- Automatic reconnection and message queueing
- MQTT 5.0 protocol support
- Integration with the `stinger-mqtt-trait` standard interface

## Architecture & Key Components

### Core Types

- **`MqttierClient`**: The main public API for MQTT operations
- **`Connection`**: Enum representing connection types (Tcp, UnixSocket, TcpLocalhost)
- **`MqttierClientBuilder`**: Builder pattern for client configuration
- **`ReceivedMessage`**: Represents an incoming MQTT message
- **`MqttierError`**: Error type for the library

### Internal State Management

- **`PublishState`**: Tracks pending publishes and acknowledgments
- **`QueuedMessage`**: Messages queued before connection
- **`QueuedSubscription`**: Subscriptions queued before connection
- **`PendingPublish`**: Messages awaiting broker acknowledgment

### Key Features by Module

- **Async runtime**: Built on `tokio` with `async_trait` for trait implementations
- **Message handling**: Uses `mpsc` channels for subscribe callbacks and `oneshot` for publish acknowledgments
- **Trait compliance**: Implements `Mqtt5PubSub` trait from `stinger-mqtt-trait`
- **Metrics** (optional): `metrics` feature flag enables telemetry collection (see `src/metrics.rs`)

## Working with the Codebase

### File Structure

```
src/
├── lib.rs          # Main implementation, most code is here
└── metrics.rs      # Optional metrics collection (feature-gated)
examples/           # Demo applications
```

### Dependencies to Know

- **rumqttc**: Underlying MQTT client (v0.24)
- **tokio**: Async runtime with full features enabled
- **stinger-mqtt-trait**: Standard MQTT trait interface
- **tracing**: Logging infrastructure
- **thiserror**: Error types
- **derive_builder**: Builder pattern generation
- **serde/serde_json**: Serialization (dev-dependencies primarily)

### Feature Flags

- **`metrics`**: Enables telemetry collection in `src/metrics.rs`

When working on features, respect existing feature gates and avoid unconditional dependencies on optional features.

## Code Guidelines & Conventions

### Error Handling

- Use `thiserror` for custom error types
- Return `Result<T>` (aliased to `std::result::Result<T, MqttierError>`)
- Map external errors (rumqttc, channel errors) appropriately
- Prefer `MqttierError` for library-level errors

### Logging

Use the `tracing` crate for structured logging:
- `debug!()` for low-level diagnostic info
- `info!()` for important events (connection, subscribe, publish)
- `warn!()` for recoverable issues
- `error!()` for failures

Example:
```rust
info!("Subscribing to topic: {}", topic);
debug!("QoS level: {}", qos);
error!("Failed to send message: {}", err);
```

### Async Patterns

- All I/O operations are async
- Use `tokio::spawn()` for background tasks
- Prefer `tokio::sync::mpsc` (bounded) over `std::sync::mpsc`
- Use `Arc<RwLock<T>>` for shared mutable state read by many tasks
- Use `Arc<Mutex<T>>` for shared mutable state with exclusive write access
- Channels for decoupled async components

### Trait Implementations

The library implements `Mqtt5PubSub` from `stinger-mqtt-trait`. When modifying or adding methods:
1. Check if they should be part of the trait or standalone
2. Keep trait methods focused on publish/subscribe operations
3. Document any deviations from trait semantics

### QoS Handling

The library works with two QoS types:
- `rumqttc::v5::mqttbytes::QoS`: Low-level rumqttc QoS
- `stinger_mqtt_trait::message::QoS`: Trait-level QoS

Convert appropriately between them when bridging trait and implementation.

## Common Tasks

### Adding a New Public Method

1. Add to `MqttierClient` (not MqttierClientBuilder unless it's a builder option)
2. Document with rustdoc comments including examples
3. Add async/await if it involves I/O
4. Return `Result<T>` using `MqttierError`
5. Add appropriate logging
6. Consider if it should integrate with the trait

### Modifying Message Flow

The message flow is:
1. User calls `publish_*()` → message queued or sent immediately
2. `PublishState` sends to broker
3. Broker returns acknowledgment (PUBACK/PUBCOMP)
4. `PendingPublish` completion sender notified
5. User's oneshot receiver resolves

When modifying this flow:
- Update both `PublishState` and the event loop
- Consider feature-gated metrics collection
- Maintain the publish queue for offline scenarios

### Working with Subscriptions

Subscriptions follow this flow:
1. User calls `subscribe()` with an mpsc::Sender
2. If connected, subscribe immediately; if not, queue
3. For each incoming message on that topic, send `ReceivedMessage` to the channel
4. Subscription ID (`usize`) returned for later unsubscribe

When modifying:
- Track subscription IDs with atomic counters
- Maintain mapping of subscription ID → (topic, sender)
- Handle sender errors gracefully (sender dropped = client no longer interested)

### Testing

- Use `examples/` directory for integration tests
- Focus on async correctness (use `tokio::test`)
- Test reconnection scenarios
- Test offline queueing behavior
- Use `tracing_subscriber` to enable logging in tests

## Performance Considerations

1. **Bounded channels**: All mpsc channels are bounded to prevent memory exhaustion
2. **Backpressure**: Respected via channel bounds; callers must handle send errors
3. **Metrics collection**: Guarded by feature flag to avoid overhead
4. **Atomic counters**: Used for subscription IDs (lock-free)
5. **Arc sharing**: Minimize clones; reuse references when possible

## Known Limitations & TODOs

1. TLS and WebSocket connections not yet supported (see `Connection` enum)
2. Metrics feature is optional but incomplete
3. No built-in request/response pattern (users must manage correlation IDs)

## Testing Checklist

When submitting changes:
- [ ] Code compiles with `cargo build`
- [ ] Tests pass with `cargo test`
- [ ] No clippy warnings: `cargo clippy`
- [ ] Documentation builds: `cargo doc --no-deps`
- [ ] Runs example successfully: `cargo run --example basic_usage`
- [ ] Feature gates work: `cargo build --features metrics`

## Documentation Standards

- **Public APIs**: Rustdoc comments with examples
- **Complex logic**: Inline comments explaining the "why"
- **Trait implementations**: Reference the trait documentation
- **Async behavior**: Clearly document any blocking or potentially slow operations

## Quick Reference Commands

```bash
# Build the library
cargo build

# Run tests
cargo test

# Check for issues
cargo clippy

# Generate documentation
cargo doc --no-deps --open

# Run an example
cargo run --example basic_usage

# Build with metrics
cargo build --features metrics
```

## Common Debugging Patterns

1. **Enable logging**: Set `RUST_LOG=mqttier=debug` before running
2. **Connection issues**: Check broker connectivity and credentials first
3. **Message not received**: Verify subscription was successful before publishing
4. **Deadlocks**: Use `timeout()` on channel operations to detect
5. **Memory leaks**: Profile with `valgrind` or `heaptrack` if suspected

## When to Escalate to Human Review

- Significant architectural changes
- Changes to the public API surface
- Performance optimizations that trade readability for speed
- New dependencies
- Changes to the trait implementation
- Security-related modifications

---

**Last Updated**: 2026-02-20
