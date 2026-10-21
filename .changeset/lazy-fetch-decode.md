---
'@kafkats/client': minor
'@kafkats/flow': minor
'@kafkats/codec-zod': minor
'@kafkats/flow-state-lmdb': minor
---

Decode fetched records shortly before they are delivered instead of when the Fetch response arrives. The consumer now
keeps each partition's record batches as fetched, compressed, and reads only their headers up front, like the Java
client's CompletedFetch. The records next in line, about one `maxBytesPerPartition` budget, are decompressed and parsed
in the background while handlers run, so decoding still overlaps handler I/O; the rest stays compressed, and data
dropped by pause, seek or a rebalance before it is next in line is never decoded. With I/O-bound handlers at the
default `partitionConcurrency` of 1, throughput stays within 8% of 0.13.0; with no-op handlers the consumer spends
about 3% more main-thread CPU per fetched MiB.

The fetch buffer now counts the bytes it actually holds: fetched batches at their wire size, at most one partly
decoded, decompressed batch per partition, and the records decoded ahead at their estimated size. The default bound
stays 10 × `maxBytesPerPartition`, and is no longer exceeded by responses that decode larger than they were fetched.
Priority strategies see `bufferedBytes` and `bufferCapacityBytes` in the same fetched bytes; `byteBudget` and `maxBytes`
still count decoded records, and a new per-partition `deliverableBytes` estimates the buffered records in those units,
scaling batches not yet decompressed by the partition's last seen compression ratio. The built-in `strict` and
`weighted` strategies use it for poll shares and tier gating. With a strategy set, the first record a partition
fetches is decoded on arrival to learn that ratio.

Corrupt record data found by the client (CRC mismatch, bad framing, a batch that fails to decompress) is now raised
like a broker `CORRUPT_MESSAGE`: the records before the bad batch are delivered first, the error names the bad batch's
offset, and the fetch position is left at the first record not delivered, so seeking past the batch resumes
consumption. The partition is not fetched again while the bad batch waits behind those records. Previously it was
logged and the partition was re-fetched in a loop.
