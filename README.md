# OBVFUTPORT V2 Passive Shadow

Hurst-style compact-state architecture test for OBVFUTPORT.

This package is intentionally isolated from OBVFUTPORT v1:

- reads the shared websocket live-batch file in read-only mode
- uses the Hurst 212-symbol universe as its stress universe
- writes only under its own `state_dir`
- does not publish to Compass
- does not write v1 ledgers, v1 caches, or broker-facing state
- does not place broker orders

The first version is a passive architecture probe. It keeps compact online OBV
state per signal/execution target and emits decision telemetry/events for the
frozen OBVFUTPORT entry logic. Promotion to canonical paper ledgers requires
archive parity against v1/frozen replay first.

