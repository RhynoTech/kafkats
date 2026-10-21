---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Serve every backlogged partition: a partition whose fetch returned data moves behind the others, and Fetch requests keep that order, so the first backlogged partition on a broker no longer takes every response while the partitions after it starve. Partitions that starved before now share fetches and handler slots with the backlog, so a backlog next to many low-volume partitions drains more slowly: in one test with `partitionConcurrency: 10` and handlers that cost about 70 ms per batch, a backlogged topic beside 80 trickling partitions fell from about 4,880 to about 1,300 records/s. Raise `partitionConcurrency` if a backlog has to keep its old rate.
