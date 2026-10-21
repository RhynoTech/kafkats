---
'@kafkats/client': patch
'@kafkats/flow': patch
'@kafkats/codec-zod': patch
'@kafkats/flow-state-lmdb': patch
---

Spend less event-loop time decoding and encoding record batches. The native `snappy` library is now called synchronously for blocks of up to 64 KiB decoded (32 KiB to compress), which removes a libuv threadpool round trip per Xerial chunk or one-record batch: on JSON records, consumer main-thread CPU per wire MiB drops from about 15 to 6-7 ms for Java-produced snappy batches and from 34-38 to 6-8 ms for broker-recompressed one-record batches. The built-in gzip codec now uses Node's async zlib above 8 KiB of input or 64 KiB of output, so a 1 MiB gzip producer batch no longer blocks the event loop for about 10 ms. Fetch responses are decoded in one pass, without re-decoding the first compressed batch, and decoding yields to the event loop every 2 ms. The pure-JS CRC32C fallback used without `@node-rs/crc32` is about twice as fast, and the client logs an info line recommending `@node-rs/crc32` when it is missing. `gzipCodec.compress()` and `decompress()` now reject on corrupt input instead of throwing synchronously.
