---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Decode fetched records as they are delivered instead of when the Fetch response arrives. The consumer now keeps each
partition's record batches as fetched, compressed, reads only their headers up front, and decompresses and parses a
batch when a poll takes records from it, like the Java client's CompletedFetch. Data dropped by pause, seek or a
rebalance is never decoded.

The fetch buffer now counts the bytes it actually holds: fetched batches at their wire size plus at most one partly
delivered, decompressed batch per partition. The default bound stays 10 × `maxBytesPerPartition`, and is no longer
exceeded by responses that decode larger than they were fetched. Priority strategies see `bufferedBytes` and
`bufferCapacityBytes` in the same fetched bytes; `byteBudget` and `maxBytes` still count decoded records.

Corrupt record data found by the client (CRC mismatch, bad framing, a batch that fails to decompress) is now raised
from the next poll like a broker `CORRUPT_MESSAGE`, after the records before it are delivered, with the fetch position
left at the bad batch. Previously it was logged and the partition was re-fetched in a loop.
