# Alarms Synchronizer

An application to watch the Controls and Phoebus alarms servers and pass user actions between them.

## Service scope and synchronization semantics

This service synchronizes user-intent-bearing alarm actions for EPICS devices that exist in both systems.

### In-scope devices

An EPICS device is in scope only when Phoebus has emitted configuration metadata for it.

- The Phoebus configuration record is the source-of-truth boundary for whether an EPICS device matters to this service.
- Controls-side EPICS updates without matching Phoebus metadata are treated as out of scope, not as missing data that should be retried indefinitely.
- The [`PvCache`](src/models/mod.rs) is therefore more than a convenience cache: it is the synchronizer's runtime record of which devices are eligible for synchronization.
- New devices can become in scope at runtime when Phoebus emits new configuration records after startup.

### Cache roles

The service currently uses two shared caches with distinct meanings:

- [`PvCache`](src/models/mod.rs): tracks the latest Phoebus configuration metadata for each in-scope EPICS device and therefore defines the in-scope device set.
- [`AlarmStateCache`](src/models/mod.rs): tracks the latest in-scope alarm-handling state observed by the synchronizer for loop prevention and duplicate suppression.

`AlarmStateCache` does **not** mean "latest successfully mirrored state". It records the latest observed in-scope state even when an outbound publish or RPC attempt fails, because that local memory helps prevent synchronization echo loops.

### Synchronization-relevant Phoebus messages

Phoebus messages are not all equally relevant:

- configuration records define in-scope devices and carry bypass/snooze semantics via the `enabled` field
- command-topic messages are relevant for acknowledgement semantics
- other Phoebus message classes are treated as non-sync noise unless a later stage explicitly promotes them

### Structured outcomes

The code now uses [`SyncOutcome`](src/models/mod.rs) to describe what happened while handling a message.

This distinguishes between cases such as:

- duplicate observations
- ignored non-sync traffic
- out-of-scope devices
- skipped work because capability or routing was unavailable
- attempted synchronization
- startup hydration

Stage 1 keeps the existing anti-loop behavior while making those semantics explicit for later stages.

## Note to developers
This template comes with a `devcontainer.json` file, which references a prebuilt development container that should have all the necessary tools for developing in Rust. Please make use of this. Install the "Dev Containers" extension in VS Code and you should be prompted to reopen the project in the container. This will save you the headache of having to install things yourself, and will enforce tool versions across different developer machines.

## Docs
The Rust documentation and a getting-started guide can be found [here](https://doc.rust-lang.org/book/title-page.html).
