---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Report fetch positions that `autoOffsetReset` moves after OFFSET_OUT_OF_RANGE: the consumer logs a warning and emits an `offsetReset` event with the topic, partition, and the offsets it moved from and to, so records skipped because retention deleted them are no longer invisible.
