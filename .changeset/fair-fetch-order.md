---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Serve every backlogged partition: a partition whose fetch returned records moves behind the others, and Fetch requests keep that order, so the first backlogged partition on a broker no longer takes every response while the partitions after it starve.
