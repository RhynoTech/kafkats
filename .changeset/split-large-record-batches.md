---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Split a partition's record batch that would be larger than the new `maxRecordBatchBytes` producer option (default 1,048,576 encoded bytes) into several batches instead of letting the broker reject it with `MessageTooLarge`. One `send()` puts all of its records for a partition into one batch, so a large send to one key used to fail, and abort its transaction, once its compressed size passed the broker's default `max.message.bytes`. The pieces are sent one after another in order with contiguous sequence numbers; inside a transaction every piece belongs to the same transaction. Outside a transaction the pieces are separate appends, so a failure can leave the earlier pieces written.

As in the Java client, the producer estimates each batch's compressed size from a per-topic compression ratio and cuts before encoding, checks the exact encoded size before sending, and splits a batch the broker still rejects as too large (`MESSAGE_TOO_LARGE` or `RECORD_LIST_TOO_LARGE`) without using a retry, narrowing down that topic's limit for later sends until five minutes after its last rejection, so a raised `max.message.bytes` takes effect. A single record that encodes larger than the limit now fails with `RecordTooLargeError` without being sent; raise `maxRecordBatchBytes` for topics whose `max.message.bytes` is larger. The producer trace hook reports `batch_split` events and the estimated size of each encoded batch.

Aborting a transaction now also discards pieces still queued, waits until nothing is queued or in flight before sending `EndTxn(abort)`, and no longer sends records queued behind a record that already failed. This also fixes `EndTxn(abort)` racing a produce request still in flight to another broker when one partition of a send failed.
