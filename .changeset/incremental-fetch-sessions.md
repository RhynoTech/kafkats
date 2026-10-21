---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Use KIP-227 incremental fetch sessions in the consumer. After the first Fetch to a broker, requests name only the partitions whose position changed, responses omit partitions with nothing new, and the broker serves partitions that just returned records after the others, so one backlogged partition no longer takes every response while the partitions behind it starve. Sessions reset to a full fetch on session errors and failed requests, fall back to sessionless fetches if the broker answers `UNSUPPORTED_VERSION`, and are closed when the consumer stops. Adds `Broker.closeFetchSession()`.
