---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Split a partition's record batch that encodes larger than 1 MiB into several batches instead of letting the broker reject it with `MessageTooLarge`. One `send()` puts all of its records for a partition into one batch, so a large send to one key used to fail (and abort its transaction) once its compressed size passed the broker's default `max.message.bytes`. The producer now checks the exact encoded size, cuts the records by size, and sends the pieces one after another in order with contiguous sequence numbers; inside a transaction every piece belongs to the same transaction. A single record is still sent unchanged. Outside a transaction the pieces are separate appends, so a failure can leave the earlier pieces written.

Aborting a transaction now also discards pieces still queued, waits until nothing is queued or in flight before sending `EndTxn(abort)`, and no longer sends records queued behind a record that already failed. This also fixes `EndTxn(abort)` racing a produce request still in flight to another broker when one partition of a send failed.
