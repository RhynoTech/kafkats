---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Reconnect the consumer's fetcher after a failed Fetch. It kept retrying the broker whose connection had dropped, without waiting, so after a broker restart the consumer stopped delivering. It now refreshes metadata and looks the partition leader up again.
